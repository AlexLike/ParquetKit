// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_array::builder::ArrayBuilder;
use arrow_schema::{Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};

use crate::schema::field_schema_to_arrow;
use crate::value::{append_value, make_builder};
use crate::{
  ColumnConfig, ColumnValue, Compression, Encoding, FieldSchema, ParquetError, WriterConfig,
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

fn to_parquet_encoding(e: &Encoding) -> basic::Encoding {
  match e {
    Encoding::Plain => basic::Encoding::PLAIN,
    Encoding::DeltaBinaryPacked => basic::Encoding::DELTA_BINARY_PACKED,
    Encoding::DeltaLengthByteArray => basic::Encoding::DELTA_LENGTH_BYTE_ARRAY,
    Encoding::DeltaByteArray => basic::Encoding::DELTA_BYTE_ARRAY,
    Encoding::RleDictionary => basic::Encoding::RLE_DICTIONARY,
  }
}
