// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_array::builder::ArrayBuilder;
use arrow_schema::{Field, Schema};
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use parquet::basic;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder, WriterVersion};

use crate::schema::{field_schema_to_arrow, primitive_to_data_type};
use crate::value::{append_value, make_builder};
use crate::{
  ColumnConfig, ColumnValue, Compression, Encoding, FieldSchema, ParquetError, PrimitiveType,
  WriterConfig,
};

#[derive(uniffi::Object)]
pub struct WriterHandle {
  writer: Mutex<ArrowWriter<File>>,
  builders: Mutex<Vec<Box<dyn ArrayBuilder>>>,
  schema: Arc<Schema>,
  row_group_size: u64,
  row_count: AtomicU64,
}

#[uniffi::export]
impl WriterHandle {
  #[uniffi::constructor]
  pub fn new(
    path: String,
    schema: Vec<FieldSchema>,
    config: WriterConfig,
  ) -> Result<Arc<Self>, ParquetError> {
    let arrow_fields: Vec<Field> = schema
      .iter()
      .map(field_schema_to_arrow)
      .collect::<Result<_, _>>()?;
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    // Validate column config names against schema.
    let field_names: std::collections::HashSet<&str> = arrow_schema
      .fields()
      .iter()
      .map(|f| f.name().as_str())
      .collect();
    for cc in &config.column_configs {
      if !field_names.contains(cc.column_name.as_str()) {
        return Err(ParquetError::Schema {
          msg: format!("unknown column name in config: '{}'", cc.column_name),
        });
      }
    }

    // Validate encoding compatibility against the column's physical type.
    for cc in &config.column_configs {
      if let Some(ref enc) = cc.encoding {
        validate_encoding_for_schema(&schema, enc, &cc.column_name)?;
      }
    }

    let props = build_writer_properties(&config, &arrow_schema)?;
    let file = File::create(&path).map_err(|e| ParquetError::Io {
      msg: format!("failed to create file '{path}': {e}"),
    })?;
    let writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).map_err(|e| {
      ParquetError::Io {
        msg: format!("failed to create ArrowWriter: {e}"),
      }
    })?;

    let capacity = config.row_group_size as usize;
    let builders: Vec<Box<dyn ArrayBuilder>> = arrow_schema
      .fields()
      .iter()
      .map(|f| make_builder(f.data_type(), capacity))
      .collect::<Result<_, _>>()?;

    Ok(Arc::new(WriterHandle {
      writer: Mutex::new(writer),
      builders: Mutex::new(builders),
      schema: arrow_schema,
      row_group_size: config.row_group_size as u64,
      row_count: AtomicU64::new(0),
    }))
  }

  pub fn append_row(&self, values: Vec<ColumnValue>) -> Result<(), ParquetError> {
    let mut builders = self.builders.lock().map_err(|e| ParquetError::Io {
      msg: format!("builders lock poisoned: {e}"),
    })?;

    if values.len() != builders.len() {
      return Err(ParquetError::Schema {
        msg: format!("expected {} values, got {}", builders.len(), values.len()),
      });
    }

    for (builder, value) in builders.iter_mut().zip(values.iter()) {
      append_value(builder.as_mut(), value)?;
    }

    let count = self.row_count.fetch_add(1, Ordering::Relaxed) + 1;

    // Auto-flush when we hit the row group size.
    if count.is_multiple_of(self.row_group_size) {
      self.flush_locked(&mut builders)?;
    }

    Ok(())
  }

  pub fn close(&self) -> Result<(), ParquetError> {
    // Flush any remaining rows.
    {
      let mut builders = self.builders.lock().map_err(|e| ParquetError::Io {
        msg: format!("builders lock poisoned: {e}"),
      })?;
      let count = self.row_count.load(Ordering::Relaxed);
      if !count.is_multiple_of(self.row_group_size) {
        self.flush_locked(&mut builders)?;
      }
    }

    let mut writer = self.writer.lock().map_err(|e| ParquetError::Io {
      msg: format!("writer lock poisoned: {e}"),
    })?;

    // ArrowWriter::close takes ownership, but we have it behind Mutex.
    // We need to use finish() instead which doesn't consume self.
    writer.finish().map_err(|e| ParquetError::Io {
      msg: format!("failed to close writer: {e}"),
    })?;

    Ok(())
  }
}

impl WriterHandle {
  /// Flush current builders into a RecordBatch and write it.
  /// Caller must hold the builders lock.
  fn flush_locked(&self, builders: &mut Vec<Box<dyn ArrayBuilder>>) -> Result<(), ParquetError> {
    let arrays: Vec<Arc<dyn arrow_array::Array>> =
      builders.iter_mut().map(|b| b.finish()).collect();

    // Don't write empty batches.
    if arrays.first().is_some_and(|a| a.is_empty()) {
      return Ok(());
    }

    let batch =
      RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| ParquetError::Io {
        msg: format!("failed to create RecordBatch: {e}"),
      })?;

    let mut writer = self.writer.lock().map_err(|e| ParquetError::Io {
      msg: format!("writer lock poisoned: {e}"),
    })?;
    writer.write(&batch).map_err(|e| ParquetError::Io {
      msg: format!("failed to write batch: {e}"),
    })?;

    Ok(())
  }
}

fn build_writer_properties(
  config: &WriterConfig,
  schema: &Schema,
) -> Result<WriterProperties, ParquetError> {
  let mut builder = WriterProperties::builder()
    .set_writer_version(WriterVersion::PARQUET_2_0)
    .set_compression(to_parquet_compression(&config.compression)?)
    .set_max_row_group_row_count(Some(config.row_group_size as usize))
    .set_data_page_size_limit(config.data_page_size as usize)
    .set_dictionary_enabled(config.enable_dictionary)
    .set_statistics_enabled(if config.enable_statistics {
      parquet::file::properties::EnabledStatistics::Page
    } else {
      parquet::file::properties::EnabledStatistics::None
    });

  builder = apply_column_configs(builder, &config.column_configs, schema)?;

  Ok(builder.build())
}

fn apply_column_configs(
  mut builder: WriterPropertiesBuilder,
  configs: &[ColumnConfig],
  _schema: &Schema,
) -> Result<WriterPropertiesBuilder, ParquetError> {
  for cc in configs {
    let col_path = parquet::schema::types::ColumnPath::new(vec![cc.column_name.clone()]);

    if let Some(ref comp) = cc.compression {
      builder = builder.set_column_compression(col_path.clone(), to_parquet_compression(comp)?);
    }
    if let Some(ref enc) = cc.encoding {
      builder = builder.set_column_encoding(col_path.clone(), to_parquet_encoding(enc));
    }
    if let Some(dict) = cc.enable_dictionary {
      builder = builder.set_column_dictionary_enabled(col_path, dict);
    }
  }
  Ok(builder)
}

fn to_parquet_compression(c: &Compression) -> Result<basic::Compression, ParquetError> {
  match c {
    Compression::None => Ok(basic::Compression::UNCOMPRESSED),
    Compression::Snappy => Ok(basic::Compression::SNAPPY),
    Compression::Lz4 => Ok(basic::Compression::LZ4),
    Compression::Gzip { level } => {
      let lvl = basic::GzipLevel::try_new(*level as u32).map_err(|e| ParquetError::Schema {
        msg: format!("invalid gzip level: {e}"),
      })?;
      Ok(basic::Compression::GZIP(lvl))
    }
    Compression::Zstd { level } => {
      let lvl = basic::ZstdLevel::try_new(*level as i32).map_err(|e| ParquetError::Schema {
        msg: format!("invalid zstd level: {e}"),
      })?;
      Ok(basic::Compression::ZSTD(lvl))
    }
    Compression::Brotli { level } => {
      let lvl = basic::BrotliLevel::try_new(*level as u32).map_err(|e| ParquetError::Schema {
        msg: format!("invalid brotli level: {e}"),
      })?;
      Ok(basic::Compression::BROTLI(lvl))
    }
  }
}

fn field_schema_name(f: &FieldSchema) -> &str {
  match f {
    FieldSchema::Primitive { name, .. } => name,
    FieldSchema::List { name, .. } => name,
    FieldSchema::Struct { name, .. } => name,
    FieldSchema::Map { name, .. } => name,
  }
}

/// Returns the Parquet physical type the crate uses to store this logical type,
/// derived authoritatively via `ArrowSchemaConverter` — the same path the
/// writer takes internally.
fn physical_type_of(pt: &PrimitiveType) -> Result<basic::Type, ParquetError> {
  let field = Field::new("v", primitive_to_data_type(pt), false);
  let schema = Schema::new(vec![field]);
  let descr = ArrowSchemaConverter::new()
    .convert(&schema)
    .map_err(|e| ParquetError::Schema {
      msg: format!("could not determine Parquet physical type: {e}"),
    })?;
  Ok(descr.column(0).physical_type())
}

fn encoding_label(e: &Encoding) -> &'static str {
  match e {
    Encoding::Plain => "PLAIN",
    Encoding::RleDictionary => "RLE_DICTIONARY",
    Encoding::DeltaBinaryPacked => "DELTA_BINARY_PACKED",
    Encoding::DeltaLengthByteArray => "DELTA_LENGTH_BYTE_ARRAY",
    Encoding::DeltaByteArray => "DELTA_BYTE_ARRAY",
    Encoding::ByteStreamSplit => "BYTE_STREAM_SPLIT",
  }
}

/// Returns the human-readable list of allowed Parquet physical types for `encoding`.
fn allowed_physical_types(e: &Encoding) -> &'static str {
  match e {
    Encoding::Plain | Encoding::RleDictionary => "all",
    Encoding::DeltaBinaryPacked => "INT32, INT64",
    Encoding::DeltaLengthByteArray => "BYTE_ARRAY",
    Encoding::DeltaByteArray => "BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY",
    Encoding::ByteStreamSplit => "INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY",
  }
}

/// Returns `Err` when `encoding` is incompatible with the Parquet physical type
/// of the column named `column_name` in `schema`.
///
/// Restrictions (from the Parquet spec):
///   DELTA_BINARY_PACKED   → INT32, INT64
///   DELTA_LENGTH_BYTE_ARRAY → BYTE_ARRAY
///   DELTA_BYTE_ARRAY      → BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
///   BYTE_STREAM_SPLIT     → INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY
///   PLAIN / RLE_DICTIONARY → all types
fn validate_encoding_for_schema(
  schema: &[FieldSchema],
  encoding: &Encoding,
  column_name: &str,
) -> Result<(), ParquetError> {
  // PLAIN and RLE_DICTIONARY accept every type; skip the check entirely.
  if matches!(encoding, Encoding::Plain | Encoding::RleDictionary) {
    return Ok(());
  }

  let field = schema.iter().find(|f| field_schema_name(f) == column_name);

  let primitive_type = match field {
    None => return Ok(()), // column-name check already caught unknown names
    Some(FieldSchema::Primitive { r#type, .. }) => r#type,
    Some(_) => {
      // Non-primitive columns (list/struct/map): restricted encodings don't apply.
      return Err(ParquetError::Schema {
        msg: format!(
          "{} encoding cannot be applied to non-primitive column '{column_name}'",
          encoding_label(encoding)
        ),
      });
    }
  };

  let physical = physical_type_of(primitive_type)?;
  let compatible = match encoding {
    Encoding::Plain | Encoding::RleDictionary => true, // already returned above
    Encoding::DeltaBinaryPacked => {
      matches!(physical, basic::Type::INT32 | basic::Type::INT64)
    }
    Encoding::DeltaLengthByteArray => {
      matches!(physical, basic::Type::BYTE_ARRAY)
    }
    Encoding::DeltaByteArray => {
      matches!(
        physical,
        basic::Type::BYTE_ARRAY | basic::Type::FIXED_LEN_BYTE_ARRAY
      )
    }
    Encoding::ByteStreamSplit => {
      matches!(
        physical,
        basic::Type::INT32
          | basic::Type::INT64
          | basic::Type::FLOAT
          | basic::Type::DOUBLE
          | basic::Type::FIXED_LEN_BYTE_ARRAY
      )
    }
  };

  if compatible {
    Ok(())
  } else {
    Err(ParquetError::Schema {
      msg: format!(
        "{} encoding is not supported for column '{}' \
         (Parquet physical type: {}); supported physical types: {}",
        encoding_label(encoding),
        column_name,
        physical,
        allowed_physical_types(encoding),
      ),
    })
  }
}

fn to_parquet_encoding(e: &Encoding) -> basic::Encoding {
  match e {
    Encoding::Plain => basic::Encoding::PLAIN,
    Encoding::DeltaBinaryPacked => basic::Encoding::DELTA_BINARY_PACKED,
    Encoding::DeltaLengthByteArray => basic::Encoding::DELTA_LENGTH_BYTE_ARRAY,
    Encoding::DeltaByteArray => basic::Encoding::DELTA_BYTE_ARRAY,
    Encoding::RleDictionary => basic::Encoding::RLE_DICTIONARY,
    Encoding::ByteStreamSplit => basic::Encoding::BYTE_STREAM_SPLIT,
  }
}
