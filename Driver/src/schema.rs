// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};

use crate::{FieldSchema, ParquetError, PrimitiveType};

/// Convert a `PrimitiveType` to its Arrow `DataType`.
pub fn primitive_to_data_type(pt: &PrimitiveType) -> DataType {
  match pt {
    PrimitiveType::Bool => DataType::Boolean,
    PrimitiveType::Int8 => DataType::Int8,
    PrimitiveType::Int16 => DataType::Int16,
    PrimitiveType::Int32 => DataType::Int32,
    PrimitiveType::Int64 => DataType::Int64,
    PrimitiveType::UInt8 => DataType::UInt8,
    PrimitiveType::UInt16 => DataType::UInt16,
    PrimitiveType::UInt32 => DataType::UInt32,
    PrimitiveType::UInt64 => DataType::UInt64,
    PrimitiveType::Float32 => DataType::Float32,
    PrimitiveType::Float64 => DataType::Float64,
    PrimitiveType::Utf8 => DataType::Utf8,
    PrimitiveType::Bytes => DataType::Binary,
    PrimitiveType::Date32 => DataType::Date32,
    PrimitiveType::TimeMs => DataType::Time32(TimeUnit::Millisecond),
    PrimitiveType::TimeUs => DataType::Time64(TimeUnit::Microsecond),
    PrimitiveType::TimeNs => DataType::Time64(TimeUnit::Nanosecond),
    PrimitiveType::TimestampMs => DataType::Timestamp(TimeUnit::Millisecond, None),
    PrimitiveType::TimestampUs => DataType::Timestamp(TimeUnit::Microsecond, None),
    PrimitiveType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
    PrimitiveType::TimestampMsUtc => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    PrimitiveType::TimestampUsUtc => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    PrimitiveType::TimestampNsUtc => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    PrimitiveType::DurationNs => DataType::Duration(TimeUnit::Nanosecond),
    // Interval uses FixedSizeBinary(12) at the Arrow level.  The field-level
    // metadata key "parquetkit_type" = "interval" is set in field_schema_to_arrow
    // so that arrow_to_field_schema can round-trip the type correctly.
    PrimitiveType::Interval => DataType::FixedSizeBinary(12),
    PrimitiveType::Float16 => DataType::Float16,
    PrimitiveType::FixedBytes { size } => DataType::FixedSizeBinary(*size as i32),
    PrimitiveType::Decimal128 { precision, scale } => {
      DataType::Decimal128(*precision, *scale as i8)
    }
  }
}

/// Convert an Arrow `DataType` back to a `PrimitiveType`.
pub fn data_type_to_primitive(dt: &DataType) -> Result<PrimitiveType, ParquetError> {
  match dt {
    DataType::Boolean => Ok(PrimitiveType::Bool),
    DataType::Int8 => Ok(PrimitiveType::Int8),
    DataType::Int16 => Ok(PrimitiveType::Int16),
    DataType::Int32 => Ok(PrimitiveType::Int32),
    DataType::Int64 => Ok(PrimitiveType::Int64),
    DataType::UInt8 => Ok(PrimitiveType::UInt8),
    DataType::UInt16 => Ok(PrimitiveType::UInt16),
    DataType::UInt32 => Ok(PrimitiveType::UInt32),
    DataType::UInt64 => Ok(PrimitiveType::UInt64),
    DataType::Float32 => Ok(PrimitiveType::Float32),
    DataType::Float64 => Ok(PrimitiveType::Float64),
    DataType::Utf8 => Ok(PrimitiveType::Utf8),
    DataType::Binary => Ok(PrimitiveType::Bytes),
    DataType::Date32 => Ok(PrimitiveType::Date32),
    DataType::Time32(TimeUnit::Millisecond) => Ok(PrimitiveType::TimeMs),
    DataType::Time64(TimeUnit::Microsecond) => Ok(PrimitiveType::TimeUs),
    DataType::Time64(TimeUnit::Nanosecond) => Ok(PrimitiveType::TimeNs),
    // Distinguish UTC-adjusted timestamps from wall-clock ones.
    DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
      Ok(PrimitiveType::TimestampMsUtc)
    }
    DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
      Ok(PrimitiveType::TimestampUsUtc)
    }
    DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) if tz.as_ref() == "UTC" => {
      Ok(PrimitiveType::TimestampNsUtc)
    }
    // Accept any other timezone annotation (non-UTC or None) as wall-clock.
    DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(PrimitiveType::TimestampMs),
    DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(PrimitiveType::TimestampUs),
    DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(PrimitiveType::TimestampNs),
    DataType::Duration(TimeUnit::Nanosecond) => Ok(PrimitiveType::DurationNs),
    DataType::Float16 => Ok(PrimitiveType::Float16),
    DataType::FixedSizeBinary(size) => Ok(PrimitiveType::FixedBytes { size: *size as u32 }),
    DataType::Decimal128(precision, scale) => Ok(PrimitiveType::Decimal128 {
      precision: *precision,
      scale: *scale as u8,
    }),
    other => Err(ParquetError::Schema {
      msg: format!("unsupported Arrow data type: {other:?}"),
    }),
  }
}

/// Metadata key used to tag an Arrow `FixedSizeBinary(12)` field as a Parquet INTERVAL column.
const INTERVAL_META_KEY: &str = "parquetkit_type";
const INTERVAL_META_VAL: &str = "interval";

/// Convert a `FieldSchema` to an Arrow `Field`.
pub fn field_schema_to_arrow(fs: &FieldSchema) -> Result<Field, ParquetError> {
  match fs {
    FieldSchema::Primitive {
      name,
      r#type,
      nullable,
    } => {
      let dt = primitive_to_data_type(r#type);
      // Tag Interval fields with metadata so arrow_to_field_schema can round-trip
      // the type.  Without this tag a FixedSizeBinary(12) would be read back as
      // FixedBytes { size: 12 }, breaking the schema equality check in Swift.
      if matches!(r#type, PrimitiveType::Interval) {
        let meta = std::collections::HashMap::from([(
          INTERVAL_META_KEY.to_string(),
          INTERVAL_META_VAL.to_string(),
        )]);
        return Ok(Field::new(name, dt, *nullable).with_metadata(meta));
      }
      Ok(Field::new(name, dt, *nullable))
    }
    FieldSchema::List {
      name,
      fields,
      nullable,
    } => {
      let element = fields.first().ok_or_else(|| ParquetError::Schema {
        msg: "List field schema must have exactly one element".into(),
      })?;
      let child = field_schema_to_arrow(element)?;
      let dt = DataType::List(Arc::new(child));
      Ok(Field::new(name, dt, *nullable))
    }
    FieldSchema::Struct {
      name,
      fields,
      nullable,
    } => {
      let arrow_fields: Vec<Field> = fields
        .iter()
        .map(field_schema_to_arrow)
        .collect::<Result<_, _>>()?;
      let dt = DataType::Struct(arrow_fields.into());
      Ok(Field::new(name, dt, *nullable))
    }
    FieldSchema::Map {
      name,
      fields,
      nullable,
    } => {
      if fields.len() != 2 {
        return Err(ParquetError::Schema {
          msg: format!(
            "Map field schema must have exactly 2 elements, got {}",
            fields.len()
          ),
        });
      }
      let key_arrow = field_schema_to_arrow(&fields[0])?;
      let val_arrow = field_schema_to_arrow(&fields[1])?;
      // Use the user-supplied field names (e.g. "key"/"value") in the entries struct.
      let entries_dt = DataType::Struct(
        vec![
          Field::new(key_arrow.name(), key_arrow.data_type().clone(), false),
          Field::new(val_arrow.name(), val_arrow.data_type().clone(), true),
        ]
        .into(),
      );
      let entries_field = Field::new("entries", entries_dt, false);
      Ok(Field::new(
        name,
        DataType::Map(Arc::new(entries_field), false),
        *nullable,
      ))
    }
  }
}

/// Convert an Arrow `Field` back to a `FieldSchema`.
pub fn arrow_to_field_schema(field: &Field) -> Result<FieldSchema, ParquetError> {
  let name = field.name().clone();
  let nullable = field.is_nullable();

  match field.data_type() {
    DataType::List(child_field) => {
      let element = arrow_to_field_schema(child_field)?;
      Ok(FieldSchema::List {
        name,
        fields: vec![element],
        nullable,
      })
    }
    DataType::Struct(struct_fields) => {
      let fields: Vec<FieldSchema> = struct_fields
        .iter()
        .map(|f| arrow_to_field_schema(f.as_ref()))
        .collect::<Result<_, _>>()?;
      Ok(FieldSchema::Struct {
        name,
        fields,
        nullable,
      })
    }
    DataType::Map(entries_field, _) => {
      // The entries field has type Struct([key_field, value_field]).
      let entries_dt = entries_field.data_type();
      if let DataType::Struct(struct_fields) = entries_dt {
        if struct_fields.len() != 2 {
          return Err(ParquetError::Schema {
            msg: "Map entries struct must have exactly 2 fields (key, value)".into(),
          });
        }
        let key_schema = arrow_to_field_schema(struct_fields[0].as_ref())?;
        let val_schema = arrow_to_field_schema(struct_fields[1].as_ref())?;
        Ok(FieldSchema::Map {
          name,
          fields: vec![key_schema, val_schema],
          nullable,
        })
      } else {
        Err(ParquetError::Schema {
          msg: "Map entries field must be a struct".into(),
        })
      }
    }
    dt => {
      // FixedSizeBinary(12) tagged with our interval metadata is an Interval column.
      if let DataType::FixedSizeBinary(12) = dt
        && field.metadata().get(INTERVAL_META_KEY).map(String::as_str) == Some(INTERVAL_META_VAL)
      {
        return Ok(FieldSchema::Primitive {
          name,
          r#type: PrimitiveType::Interval,
          nullable,
        });
      }
      let pt = data_type_to_primitive(dt)?;
      Ok(FieldSchema::Primitive {
        name,
        r#type: pt,
        nullable,
      })
    }
  }
}

/// Convert a slice of `FieldSchema` to an Arrow `Schema`.
#[cfg(test)]
pub fn schema_to_arrow(fields: &[FieldSchema]) -> Result<arrow_schema::Schema, ParquetError> {
  let arrow_fields: Vec<Field> = fields
    .iter()
    .map(field_schema_to_arrow)
    .collect::<Result<_, _>>()?;
  Ok(arrow_schema::Schema::new(arrow_fields))
}

/// Convert an Arrow `Schema` back to `Vec<FieldSchema>`.
pub fn arrow_to_schema(schema: &arrow_schema::Schema) -> Result<Vec<FieldSchema>, ParquetError> {
  schema
    .fields()
    .iter()
    .map(|f| arrow_to_field_schema(f.as_ref()))
    .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::panic)]
mod tests {
  use super::*;

  #[test]
  fn round_trip_all_primitive_types() {
    let primitives = vec![
      PrimitiveType::Bool,
      PrimitiveType::Int8,
      PrimitiveType::Int16,
      PrimitiveType::Int32,
      PrimitiveType::Int64,
      PrimitiveType::UInt8,
      PrimitiveType::UInt16,
      PrimitiveType::UInt32,
      PrimitiveType::UInt64,
      PrimitiveType::Float32,
      PrimitiveType::Float64,
      PrimitiveType::Utf8,
      PrimitiveType::Bytes,
      PrimitiveType::Date32,
      PrimitiveType::TimeMs,
      PrimitiveType::TimeUs,
      PrimitiveType::TimeNs,
      PrimitiveType::TimestampMs,
      PrimitiveType::TimestampUs,
      PrimitiveType::TimestampNs,
      PrimitiveType::TimestampMsUtc,
      PrimitiveType::TimestampUsUtc,
      PrimitiveType::TimestampNsUtc,
      PrimitiveType::DurationNs,
      // Note: Interval is intentionally omitted here; it round-trips through
      // Arrow *field* metadata (not just DataType), tested separately below.
      PrimitiveType::Float16,
      PrimitiveType::FixedBytes { size: 16 },
      PrimitiveType::Decimal128 {
        precision: 10,
        scale: 2,
      },
    ];

    for pt in &primitives {
      let dt = primitive_to_data_type(pt);
      let round_tripped = data_type_to_primitive(&dt).unwrap();
      assert_eq!(&round_tripped, pt, "failed round-trip for {pt:?}");
    }
  }

  #[test]
  fn round_trip_field_schema_primitive() {
    let fs = FieldSchema::Primitive {
      name: "count".into(),
      r#type: PrimitiveType::Int64,
      nullable: false,
    };
    let arrow = field_schema_to_arrow(&fs).unwrap();
    let back = arrow_to_field_schema(&arrow).unwrap();
    assert_eq!(fs, back);
  }

  #[test]
  fn round_trip_field_schema_list() {
    let fs = FieldSchema::List {
      name: "tags".into(),
      fields: vec![FieldSchema::Primitive {
        name: "item".into(),
        r#type: PrimitiveType::Utf8,
        nullable: false,
      }],
      nullable: true,
    };
    let arrow = field_schema_to_arrow(&fs).unwrap();
    let back = arrow_to_field_schema(&arrow).unwrap();
    assert_eq!(fs, back);
  }

  #[test]
  fn round_trip_field_schema_struct() {
    let fs = FieldSchema::Struct {
      name: "metadata".into(),
      fields: vec![
        FieldSchema::Primitive {
          name: "key".into(),
          r#type: PrimitiveType::Utf8,
          nullable: false,
        },
        FieldSchema::Primitive {
          name: "value".into(),
          r#type: PrimitiveType::Int32,
          nullable: true,
        },
      ],
      nullable: false,
    };
    let arrow = field_schema_to_arrow(&fs).unwrap();
    let back = arrow_to_field_schema(&arrow).unwrap();
    assert_eq!(fs, back);
  }

  #[test]
  fn round_trip_full_schema() {
    let schema = vec![
      FieldSchema::Primitive {
        name: "id".into(),
        r#type: PrimitiveType::Int64,
        nullable: false,
      },
      FieldSchema::List {
        name: "scores".into(),
        fields: vec![FieldSchema::Primitive {
          name: "item".into(),
          r#type: PrimitiveType::Float64,
          nullable: false,
        }],
        nullable: true,
      },
      FieldSchema::Struct {
        name: "info".into(),
        fields: vec![FieldSchema::Primitive {
          name: "name".into(),
          r#type: PrimitiveType::Utf8,
          nullable: false,
        }],
        nullable: false,
      },
    ];
    let arrow = schema_to_arrow(&schema).unwrap();
    let back = arrow_to_schema(&arrow).unwrap();
    assert_eq!(schema, back);
  }

  #[test]
  fn round_trip_field_schema_interval() {
    // Interval uses FixedSizeBinary(12) + metadata at the Arrow level.
    let fs = FieldSchema::Primitive {
      name: "period".into(),
      r#type: PrimitiveType::Interval,
      nullable: false,
    };
    let arrow = field_schema_to_arrow(&fs).unwrap();
    // Confirm the Arrow type is FixedSizeBinary(12) (not MonthDayNano).
    assert_eq!(arrow.data_type(), &DataType::FixedSizeBinary(12));
    let back = arrow_to_field_schema(&arrow).unwrap();
    assert_eq!(fs, back);
  }
}
