// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

use std::sync::Arc;

use arrow_array::builder::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};

use crate::{ColumnValue, ParquetError};

/// Append a `ColumnValue` into a dynamically-typed `ArrayBuilder`.
pub fn append_value(
  builder: &mut dyn ArrayBuilder,
  value: &ColumnValue,
) -> Result<(), ParquetError> {
  // Handle Null for any builder type.
  if matches!(value, ColumnValue::Null) {
    return append_null(builder);
  }

  let any = builder.as_any_mut();

  match value {
    ColumnValue::Null => Ok(()), // already handled above
    ColumnValue::Bool { v } => {
      let b = any
        .downcast_mut::<BooleanBuilder>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected BooleanBuilder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::Int8 { v } => {
      let b = any
        .downcast_mut::<PrimitiveBuilder<Int8Type>>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Int8Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::Int16 { v } => {
      let b = any
        .downcast_mut::<PrimitiveBuilder<Int16Type>>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Int16Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::Int32 { v } => {
      if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Int32Type>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Date32Type>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Time32MillisecondType>>() {
        b.append_value(*v);
      } else {
        return Err(ParquetError::TypeMismatch {
          msg: "expected Int32, Date32, or Time32Millisecond builder".into(),
        });
      }
      Ok(())
    }
    ColumnValue::Int64 { v } => {
      if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Int64Type>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Time64MicrosecondType>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Time64NanosecondType>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampMillisecondType>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampMicrosecondType>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampNanosecondType>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampSecondType>>() {
        b.append_value(*v);
      } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<DurationNanosecondType>>() {
        b.append_value(*v);
      } else {
        return Err(ParquetError::TypeMismatch {
          msg: "expected Int64, Time64, Timestamp, or Duration builder".into(),
        });
      }
      Ok(())
    }
    ColumnValue::UInt8 { v } => {
      let b = any
        .downcast_mut::<PrimitiveBuilder<UInt8Type>>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected UInt8Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::UInt16 { v } => {
      let b = any
        .downcast_mut::<PrimitiveBuilder<UInt16Type>>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected UInt16Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::UInt32 { v } => {
      let b = any
        .downcast_mut::<PrimitiveBuilder<UInt32Type>>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected UInt32Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::UInt64 { v } => {
      let b = any
        .downcast_mut::<PrimitiveBuilder<UInt64Type>>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected UInt64Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::Float16 { v } => {
      let b = any
        .downcast_mut::<Float16Builder>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Float16Builder".into(),
        })?;
      b.append_value(half::f16::from_f32(*v));
      Ok(())
    }
    ColumnValue::Float32 { v } => {
      let b = any
        .downcast_mut::<Float32Builder>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Float32Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::Float64 { v } => {
      let b = any
        .downcast_mut::<Float64Builder>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Float64Builder".into(),
        })?;
      b.append_value(*v);
      Ok(())
    }
    ColumnValue::Bytes { v } => {
      // Could be BinaryBuilder, FixedSizeBinaryBuilder, or Decimal128Builder.
      if let Some(b) = any.downcast_mut::<BinaryBuilder>() {
        b.append_value(v);
        Ok(())
      } else if let Some(b) = any.downcast_mut::<FixedSizeBinaryBuilder>() {
        b.append_value(v).map_err(|e| ParquetError::TypeMismatch {
          msg: format!("FixedSizeBinary append error: {e}"),
        })?;
        Ok(())
      } else if let Some(b) = any.downcast_mut::<Decimal128Builder>() {
        // Decimal128 values arrive as 16-byte big-endian buffers.
        if v.len() != 16 {
          return Err(ParquetError::TypeMismatch {
            msg: format!("Decimal128 requires 16 bytes, got {}", v.len()),
          });
        }
        let bytes: [u8; 16] =
          <[u8; 16]>::try_from(v.as_slice()).map_err(|_| ParquetError::TypeMismatch {
            msg: "invalid Decimal128 bytes".into(),
          })?;
        let i128_val = i128::from_be_bytes(bytes);
        b.append_value(i128_val);
        Ok(())
      } else {
        Err(ParquetError::TypeMismatch {
          msg: "expected BinaryBuilder, FixedSizeBinaryBuilder, or Decimal128Builder for Bytes"
            .into(),
        })
      }
    }
    ColumnValue::Utf8 { v } => {
      let b = any
        .downcast_mut::<StringBuilder>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected StringBuilder".into(),
        })?;
      b.append_value(v);
      Ok(())
    }
    ColumnValue::List { items } => {
      if let Some(b) = any.downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>() {
        let values_builder = b.values();
        for item in items {
          append_value(values_builder.as_mut(), item)?;
        }
        b.append(true);
        Ok(())
      } else if let Some(b) =
        any.downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
      {
        // Map values arrive as a list of Struct { fields: [key, value] } items.
        for item in items {
          match item {
            ColumnValue::Struct { fields } if fields.len() == 2 => {
              append_value(b.keys(), &fields[0])?;
              append_value(b.values(), &fields[1])?;
            }
            _ => {
              return Err(ParquetError::TypeMismatch {
                msg: "Map items must be Struct { fields: [key, value] }".into(),
              });
            }
          }
        }
        b.append(true).map_err(|e| ParquetError::TypeMismatch {
          msg: format!("MapBuilder append error: {e}"),
        })?;
        Ok(())
      } else {
        Err(ParquetError::TypeMismatch {
          msg: "expected ListBuilder or MapBuilder".into(),
        })
      }
    }
    ColumnValue::Struct { fields } => {
      let b = any
        .downcast_mut::<StructBuilder>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected StructBuilder".into(),
        })?;
      if fields.len() != b.num_fields() {
        return Err(ParquetError::TypeMismatch {
          msg: format!(
            "struct has {} fields but got {} values",
            b.num_fields(),
            fields.len()
          ),
        });
      }
      let field_builders = b.field_builders_mut();
      for (i, field_val) in fields.iter().enumerate() {
        append_value(field_builders[i].as_mut(), field_val)?;
      }
      b.append(true);
      Ok(())
    }
  }
}

/// Append a null to any builder type by trying each known builder type.
fn append_null(builder: &mut dyn ArrayBuilder) -> Result<(), ParquetError> {
  let any = builder.as_any_mut();

  // Try each builder type.
  if let Some(b) = any.downcast_mut::<BooleanBuilder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Int8Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Int16Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Int32Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Date32Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Time32MillisecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Int64Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Time64MicrosecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampMillisecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampMicrosecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampNanosecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<TimestampSecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<Time64NanosecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<DurationNanosecondType>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<UInt8Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<UInt16Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<UInt32Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<PrimitiveBuilder<UInt64Type>>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<Float16Builder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<Float32Builder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<Float64Builder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<StringBuilder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<BinaryBuilder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<FixedSizeBinaryBuilder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<Decimal128Builder>() {
    b.append_null();
  } else if let Some(b) = any.downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>() {
    b.append(false);
  } else if let Some(b) =
    any.downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
  {
    b.append(false).map_err(|e| ParquetError::TypeMismatch {
      msg: format!("MapBuilder null append error: {e}"),
    })?;
  } else if let Some(b) = any.downcast_mut::<StructBuilder>() {
    // Children must be advanced to the same length before append(false).
    // Collect child count first to avoid holding the borrow across the loop.
    let num_fields = b.num_fields();
    {
      let field_builders = b.field_builders_mut();
      for child in field_builders.iter_mut() {
        append_null(child.as_mut())?;
      }
    }
    let _ = num_fields; // suppress unused warning
    b.append(false);
  } else {
    return Err(ParquetError::TypeMismatch {
      msg: "unsupported builder type for null append".into(),
    });
  }
  Ok(())
}

/// Extract a `ColumnValue` from an Arrow array at a given row index.
pub fn extract_value(array: &dyn Array, row: usize) -> Result<ColumnValue, ParquetError> {
  // Null check at every level of recursion.
  if array.is_null(row) {
    return Ok(ColumnValue::Null);
  }

  match array.data_type() {
    DataType::Boolean => {
      let a = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected BooleanArray".into(),
        })?;
      Ok(ColumnValue::Bool { v: a.value(row) })
    }
    DataType::Int8 => {
      let a =
        array
          .as_any()
          .downcast_ref::<Int8Array>()
          .ok_or_else(|| ParquetError::TypeMismatch {
            msg: "expected Int8Array".into(),
          })?;
      Ok(ColumnValue::Int8 { v: a.value(row) })
    }
    DataType::Int16 => {
      let a =
        array
          .as_any()
          .downcast_ref::<Int16Array>()
          .ok_or_else(|| ParquetError::TypeMismatch {
            msg: "expected Int16Array".into(),
          })?;
      Ok(ColumnValue::Int16 { v: a.value(row) })
    }
    DataType::Int32 => {
      let a =
        array
          .as_any()
          .downcast_ref::<Int32Array>()
          .ok_or_else(|| ParquetError::TypeMismatch {
            msg: "expected Int32Array".into(),
          })?;
      Ok(ColumnValue::Int32 { v: a.value(row) })
    }
    DataType::Int64 => {
      let a =
        array
          .as_any()
          .downcast_ref::<Int64Array>()
          .ok_or_else(|| ParquetError::TypeMismatch {
            msg: "expected Int64Array".into(),
          })?;
      Ok(ColumnValue::Int64 { v: a.value(row) })
    }
    DataType::UInt8 => {
      let a =
        array
          .as_any()
          .downcast_ref::<UInt8Array>()
          .ok_or_else(|| ParquetError::TypeMismatch {
            msg: "expected UInt8Array".into(),
          })?;
      Ok(ColumnValue::UInt8 { v: a.value(row) })
    }
    DataType::UInt16 => {
      let a = array
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected UInt16Array".into(),
        })?;
      Ok(ColumnValue::UInt16 { v: a.value(row) })
    }
    DataType::UInt32 => {
      let a = array
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected UInt32Array".into(),
        })?;
      Ok(ColumnValue::UInt32 { v: a.value(row) })
    }
    DataType::UInt64 => {
      let a = array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected UInt64Array".into(),
        })?;
      Ok(ColumnValue::UInt64 { v: a.value(row) })
    }
    DataType::Float16 => {
      let a = array
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Float16Array".into(),
        })?;
      Ok(ColumnValue::Float16 {
        v: a.value(row).to_f32(),
      })
    }
    DataType::Float32 => {
      let a = array
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Float32Array".into(),
        })?;
      Ok(ColumnValue::Float32 { v: a.value(row) })
    }
    DataType::Float64 => {
      let a = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Float64Array".into(),
        })?;
      Ok(ColumnValue::Float64 { v: a.value(row) })
    }
    DataType::Utf8 => {
      let a = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected StringArray".into(),
        })?;
      Ok(ColumnValue::Utf8 {
        v: a.value(row).to_string(),
      })
    }
    DataType::Binary => {
      let a = array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected BinaryArray".into(),
        })?;
      Ok(ColumnValue::Bytes {
        v: a.value(row).to_vec(),
      })
    }
    DataType::FixedSizeBinary(_) => {
      let a = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected FixedSizeBinaryArray".into(),
        })?;
      Ok(ColumnValue::Bytes {
        v: a.value(row).to_vec(),
      })
    }
    DataType::Decimal128(_, _) => {
      let a = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Decimal128Array".into(),
        })?;
      let val = a.value(row);
      Ok(ColumnValue::Bytes {
        v: val.to_be_bytes().to_vec(),
      })
    }
    DataType::Date32 => {
      let a = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Date32Array".into(),
        })?;
      Ok(ColumnValue::Int32 { v: a.value(row) })
    }
    DataType::Time32(ArrowTimeUnit::Millisecond) => {
      let a = array
        .as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Time32MillisecondArray".into(),
        })?;
      Ok(ColumnValue::Int32 { v: a.value(row) })
    }
    DataType::Time64(ArrowTimeUnit::Microsecond) => {
      let a = array
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Time64MicrosecondArray".into(),
        })?;
      Ok(ColumnValue::Int64 { v: a.value(row) })
    }
    DataType::Time64(ArrowTimeUnit::Nanosecond) => {
      let a = array
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected Time64NanosecondArray".into(),
        })?;
      Ok(ColumnValue::Int64 { v: a.value(row) })
    }
    DataType::Timestamp(unit, _) => {
      let v = match unit {
        ArrowTimeUnit::Millisecond => {
          let a = array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| ParquetError::TypeMismatch {
              msg: "expected TimestampMillisecondArray".into(),
            })?;
          a.value(row)
        }
        ArrowTimeUnit::Microsecond => {
          let a = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| ParquetError::TypeMismatch {
              msg: "expected TimestampMicrosecondArray".into(),
            })?;
          a.value(row)
        }
        ArrowTimeUnit::Nanosecond => {
          let a = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| ParquetError::TypeMismatch {
              msg: "expected TimestampNanosecondArray".into(),
            })?;
          a.value(row)
        }
        ArrowTimeUnit::Second => {
          let a = array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .ok_or_else(|| ParquetError::TypeMismatch {
              msg: "expected TimestampSecondArray".into(),
            })?;
          a.value(row)
        }
      };
      Ok(ColumnValue::Int64 { v })
    }
    DataType::Duration(ArrowTimeUnit::Nanosecond) => {
      let a = array
        .as_any()
        .downcast_ref::<DurationNanosecondArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected DurationNanosecondArray".into(),
        })?;
      Ok(ColumnValue::Int64 { v: a.value(row) })
    }
    DataType::Map(_, _) => {
      let a =
        array
          .as_any()
          .downcast_ref::<MapArray>()
          .ok_or_else(|| ParquetError::TypeMismatch {
            msg: "expected MapArray".into(),
          })?;
      let offsets = a.value_offsets();
      let start = offsets[row] as usize;
      let end = offsets[row + 1] as usize;
      let entries = a.entries();
      let key_col = entries.column(0);
      let val_col = entries.column(1);
      let mut items = Vec::with_capacity(end - start);
      for i in start..end {
        let key_cv = extract_value(key_col.as_ref(), i)?;
        let val_cv = extract_value(val_col.as_ref(), i)?;
        items.push(ColumnValue::Struct {
          fields: vec![key_cv, val_cv],
        });
      }
      Ok(ColumnValue::List { items })
    }
    DataType::List(_) => {
      let a =
        array
          .as_any()
          .downcast_ref::<ListArray>()
          .ok_or_else(|| ParquetError::TypeMismatch {
            msg: "expected ListArray".into(),
          })?;
      let offsets = a.value_offsets();
      let start = offsets[row] as usize;
      let end = offsets[row + 1] as usize;
      let values = a.values();
      let mut items = Vec::with_capacity(end - start);
      for i in start..end {
        items.push(extract_value(values.as_ref(), i)?);
      }
      Ok(ColumnValue::List { items })
    }
    DataType::Struct(_) => {
      let a = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| ParquetError::TypeMismatch {
          msg: "expected StructArray".into(),
        })?;
      let mut fields = Vec::with_capacity(a.num_columns());
      for col in a.columns() {
        fields.push(extract_value(col.as_ref(), row)?);
      }
      Ok(ColumnValue::Struct { fields })
    }
    other => Err(ParquetError::TypeMismatch {
      msg: format!("unsupported data type for extraction: {other:?}"),
    }),
  }
}

/// Build an `ArrayBuilder` from an Arrow `DataType`.
pub fn make_builder(dt: &DataType, capacity: usize) -> Result<Box<dyn ArrayBuilder>, ParquetError> {
  match dt {
    DataType::Boolean => Ok(Box::new(BooleanBuilder::with_capacity(capacity))),
    DataType::Int8 => Ok(Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(
      capacity,
    ))),
    DataType::Int16 => Ok(Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(
      capacity,
    ))),
    DataType::Int32 => Ok(Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(
      capacity,
    ))),
    DataType::Date32 => Ok(Box::new(PrimitiveBuilder::<Date32Type>::with_capacity(
      capacity,
    ))),
    DataType::Time32(ArrowTimeUnit::Millisecond) => Ok(Box::new(PrimitiveBuilder::<
      Time32MillisecondType,
    >::with_capacity(capacity))),
    DataType::Int64 => Ok(Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(
      capacity,
    ))),
    DataType::Time64(ArrowTimeUnit::Microsecond) => Ok(Box::new(PrimitiveBuilder::<
      Time64MicrosecondType,
    >::with_capacity(capacity))),
    DataType::Time64(ArrowTimeUnit::Nanosecond) => Ok(Box::new(PrimitiveBuilder::<
      Time64NanosecondType,
    >::with_capacity(capacity))),
    DataType::Timestamp(unit, tz) => match unit {
      ArrowTimeUnit::Millisecond => Ok(Box::new(
        PrimitiveBuilder::<TimestampMillisecondType>::with_capacity(capacity)
          .with_timezone_opt(tz.clone()),
      )),
      ArrowTimeUnit::Microsecond => Ok(Box::new(
        PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(capacity)
          .with_timezone_opt(tz.clone()),
      )),
      ArrowTimeUnit::Nanosecond => Ok(Box::new(
        PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(capacity)
          .with_timezone_opt(tz.clone()),
      )),
      ArrowTimeUnit::Second => Ok(Box::new(
        PrimitiveBuilder::<TimestampSecondType>::with_capacity(capacity)
          .with_timezone_opt(tz.clone()),
      )),
    },
    DataType::Duration(ArrowTimeUnit::Nanosecond) => Ok(Box::new(PrimitiveBuilder::<
      DurationNanosecondType,
    >::with_capacity(capacity))),
    DataType::UInt8 => Ok(Box::new(PrimitiveBuilder::<UInt8Type>::with_capacity(
      capacity,
    ))),
    DataType::UInt16 => Ok(Box::new(PrimitiveBuilder::<UInt16Type>::with_capacity(
      capacity,
    ))),
    DataType::UInt32 => Ok(Box::new(PrimitiveBuilder::<UInt32Type>::with_capacity(
      capacity,
    ))),
    DataType::UInt64 => Ok(Box::new(PrimitiveBuilder::<UInt64Type>::with_capacity(
      capacity,
    ))),
    DataType::Float16 => Ok(Box::new(Float16Builder::with_capacity(capacity))),
    DataType::Float32 => Ok(Box::new(Float32Builder::with_capacity(capacity))),
    DataType::Float64 => Ok(Box::new(Float64Builder::with_capacity(capacity))),
    DataType::Utf8 => Ok(Box::new(StringBuilder::with_capacity(capacity, 64))),
    DataType::Binary => Ok(Box::new(BinaryBuilder::with_capacity(capacity, 64))),
    DataType::FixedSizeBinary(size) => Ok(Box::new(FixedSizeBinaryBuilder::with_capacity(
      capacity, *size,
    ))),
    DataType::Decimal128(precision, scale) => Ok(Box::new(
      Decimal128Builder::with_capacity(capacity)
        .with_data_type(DataType::Decimal128(*precision, *scale)),
    )),
    DataType::List(child_field) => {
      let child_builder = make_builder(child_field.data_type(), capacity)?;
      Ok(Box::new(
        ListBuilder::new(child_builder).with_field(child_field.clone()),
      ))
    }
    DataType::Struct(struct_fields) => {
      let field_builders: Vec<Box<dyn ArrayBuilder>> = struct_fields
        .iter()
        .map(|f| make_builder(f.data_type(), capacity))
        .collect::<Result<_, _>>()?;
      let fields: Vec<Arc<arrow_schema::Field>> = struct_fields.iter().map(Arc::clone).collect();
      Ok(Box::new(StructBuilder::new(fields, field_builders)))
    }
    DataType::Map(entries_field, _) => {
      let entries_dt = entries_field.data_type();
      if let DataType::Struct(struct_fields) = entries_dt {
        if struct_fields.len() != 2 {
          return Err(ParquetError::Schema {
            msg: "Map entries struct must have exactly 2 fields (key, value)".into(),
          });
        }
        let key_builder = make_builder(struct_fields[0].data_type(), capacity)?;
        let val_builder = make_builder(struct_fields[1].data_type(), capacity)?;
        // Pass the user-supplied field names so MapBuilder's generated schema
        // matches the schema we constructed in field_schema_to_arrow.
        let field_names = Some(arrow_array::builder::MapFieldNames {
          entry: entries_field.name().to_string(),
          key: struct_fields[0].name().to_string(),
          value: struct_fields[1].name().to_string(),
        });
        Ok(Box::new(MapBuilder::new(
          field_names,
          key_builder,
          val_builder,
        )))
      } else {
        Err(ParquetError::Schema {
          msg: "Map entries field must be a struct".into(),
        })
      }
    }
    other => Err(ParquetError::Schema {
      msg: format!("cannot create builder for data type: {other:?}"),
    }),
  }
}
