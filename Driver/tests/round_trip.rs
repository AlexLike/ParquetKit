// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

use parquet_swift::*;
use tempfile::NamedTempFile;

fn default_config() -> WriterConfig {
  WriterConfig {
    compression: Compression::None,
    row_group_size: 1024,
    data_page_size: 1_048_576,
    enable_dictionary: false,
    enable_statistics: true,
    column_configs: vec![],
  }
}

fn write_and_read(schema: Vec<FieldSchema>, rows: Vec<Vec<ColumnValue>>) -> Vec<Vec<ColumnValue>> {
  write_and_read_with_config(schema, rows, default_config())
}

fn write_and_read_with_config(
  schema: Vec<FieldSchema>,
  rows: Vec<Vec<ColumnValue>>,
  config: WriterConfig,
) -> Vec<Vec<ColumnValue>> {
  let tmp = NamedTempFile::new().unwrap();
  let path = tmp.path().to_str().unwrap().to_string();

  let writer = WriterHandle::new(path.clone(), schema, config).unwrap();
  for row in &rows {
    writer.append_row(row.clone()).unwrap();
  }
  writer.close().unwrap();

  let reader = ReaderHandle::new(path).unwrap();
  let mut result = vec![];
  while let Some(row) = reader.read_row().unwrap() {
    result.push(row);
  }
  reader.close();
  result
}

// ── Primitive type round-trips ──────────────────────────────────────

#[test]
fn round_trip_bool() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Bool,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Bool { v: true }],
    vec![ColumnValue::Bool { v: false }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_int8() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Int8,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Int8 { v: -128 }],
    vec![ColumnValue::Int8 { v: 127 }],
    vec![ColumnValue::Int8 { v: 0 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_int16() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Int16,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Int16 { v: i16::MIN }],
    vec![ColumnValue::Int16 { v: i16::MAX }],
    vec![ColumnValue::Int16 { v: 0 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_int32() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Int32,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Int32 { v: -2_147_483_648 }],
    vec![ColumnValue::Int32 { v: 2_147_483_647 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_int64() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Int64,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Int64 { v: i64::MIN }],
    vec![ColumnValue::Int64 { v: i64::MAX }],
    vec![ColumnValue::Int64 { v: 0 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_uint8() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::UInt8,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::UInt8 { v: 0 }],
    vec![ColumnValue::UInt8 { v: 255 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_uint16() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::UInt16,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::UInt16 { v: 0 }],
    vec![ColumnValue::UInt16 { v: u16::MAX }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_uint32() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::UInt32,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::UInt32 { v: 0 }],
    vec![ColumnValue::UInt32 { v: u32::MAX }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_uint64() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::UInt64,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::UInt64 { v: 0 }],
    vec![ColumnValue::UInt64 { v: u64::MAX }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_float32() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Float32,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Float32 { v: 1.5_f32 }],
    vec![ColumnValue::Float32 { v: 0.0 }],
    vec![ColumnValue::Float32 { v: f32::MAX }],
    vec![ColumnValue::Float32 { v: f32::INFINITY }],
    vec![ColumnValue::Float32 {
      v: f32::NEG_INFINITY,
    }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_float64() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Float64,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Float64 {
      v: std::f64::consts::PI,
    }],
    vec![ColumnValue::Float64 { v: 0.0 }],
    vec![ColumnValue::Float64 { v: f64::MAX }],
    vec![ColumnValue::Float64 { v: f64::INFINITY }],
    vec![ColumnValue::Float64 {
      v: f64::NEG_INFINITY,
    }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_utf8() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Utf8,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Utf8 {
      v: "hello world".into(),
    }],
    vec![ColumnValue::Utf8 { v: "".into() }],
    vec![ColumnValue::Utf8 {
      v: "unicode: \u{1F600}".into(),
    }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_bytes() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Bytes,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Bytes {
      v: vec![0, 1, 2, 255],
    }],
    vec![ColumnValue::Bytes { v: vec![] }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_fixed_bytes() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::FixedBytes { size: 16 },
    nullable: false,
  }];
  let uuid_bytes: Vec<u8> = (0..16).collect();
  let rows = vec![vec![ColumnValue::Bytes { v: uuid_bytes }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_date32() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Date32,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Int32 { v: 19000 }]]; // ~52 years since epoch
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_time_us() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimeUs,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Int64 { v: 43_200_000_000 }]]; // noon
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_timestamp_ms() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimestampMs,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Int64 {
    v: 1_700_000_000_000,
  }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_timestamp_us() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimestampUs,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Int64 {
    v: 1_700_000_000_000_000,
  }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_timestamp_ns() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimestampNs,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Int64 {
    v: 1_700_000_000_000_000_000,
  }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_duration_ns() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::DurationNs,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Int64 { v: 5_000_000_000 }]]; // 5 seconds
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_decimal128() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Decimal128 {
      precision: 10,
      scale: 2,
    },
    nullable: false,
  }];
  // 12345 in i128, stored as big-endian bytes
  let val: i128 = 1234500;
  let rows = vec![vec![ColumnValue::Bytes {
    v: val.to_be_bytes().to_vec(),
  }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── Nullable types ──────────────────────────────────────────────────

#[test]
fn round_trip_nullable() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Int64,
    nullable: true,
  }];
  let rows = vec![
    vec![ColumnValue::Int64 { v: 42 }],
    vec![ColumnValue::Null],
    vec![ColumnValue::Int64 { v: -1 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── List types ──────────────────────────────────────────────────────

#[test]
fn round_trip_list() {
  let schema = vec![FieldSchema::List {
    name: "tags".into(),
    fields: vec![FieldSchema::Primitive {
      name: "item".into(),
      r#type: PrimitiveType::Utf8,
      nullable: false,
    }],
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::List {
      items: vec![
        ColumnValue::Utf8 { v: "a".into() },
        ColumnValue::Utf8 { v: "b".into() },
      ],
    }],
    vec![ColumnValue::List {
      items: vec![ColumnValue::Utf8 { v: "c".into() }],
    }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_empty_list() {
  let schema = vec![FieldSchema::List {
    name: "tags".into(),
    fields: vec![FieldSchema::Primitive {
      name: "item".into(),
      r#type: PrimitiveType::Utf8,
      nullable: false,
    }],
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::List { items: vec![] }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_null_list() {
  let schema = vec![FieldSchema::List {
    name: "tags".into(),
    fields: vec![FieldSchema::Primitive {
      name: "item".into(),
      r#type: PrimitiveType::Utf8,
      nullable: false,
    }],
    nullable: true,
  }];
  let rows = vec![
    vec![ColumnValue::Null],
    vec![ColumnValue::List { items: vec![] }],
    vec![ColumnValue::List {
      items: vec![ColumnValue::Utf8 { v: "x".into() }],
    }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_null_inside_list() {
  let schema = vec![FieldSchema::List {
    name: "values".into(),
    fields: vec![FieldSchema::Primitive {
      name: "item".into(),
      r#type: PrimitiveType::Int32,
      nullable: true,
    }],
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::List {
    items: vec![
      ColumnValue::Int32 { v: 1 },
      ColumnValue::Null,
      ColumnValue::Int32 { v: 3 },
    ],
  }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── Struct types ────────────────────────────────────────────────────

#[test]
fn round_trip_struct() {
  let schema = vec![FieldSchema::Struct {
    name: "info".into(),
    fields: vec![
      FieldSchema::Primitive {
        name: "key".into(),
        r#type: PrimitiveType::Utf8,
        nullable: false,
      },
      FieldSchema::Primitive {
        name: "value".into(),
        r#type: PrimitiveType::Int32,
        nullable: false,
      },
    ],
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Struct {
    fields: vec![
      ColumnValue::Utf8 { v: "count".into() },
      ColumnValue::Int32 { v: 42 },
    ],
  }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── Multi-column rows ───────────────────────────────────────────────

#[test]
fn round_trip_multi_column() {
  let schema = vec![
    FieldSchema::Primitive {
      name: "id".into(),
      r#type: PrimitiveType::Int64,
      nullable: false,
    },
    FieldSchema::Primitive {
      name: "name".into(),
      r#type: PrimitiveType::Utf8,
      nullable: false,
    },
    FieldSchema::Primitive {
      name: "score".into(),
      r#type: PrimitiveType::Float64,
      nullable: true,
    },
  ];
  let rows = vec![
    vec![
      ColumnValue::Int64 { v: 1 },
      ColumnValue::Utf8 { v: "Alice".into() },
      ColumnValue::Float64 { v: 95.5 },
    ],
    vec![
      ColumnValue::Int64 { v: 2 },
      ColumnValue::Utf8 { v: "Bob".into() },
      ColumnValue::Null,
    ],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── Schema validation ───────────────────────────────────────────────

#[test]
fn unknown_column_config_returns_schema_error() {
  let schema = vec![FieldSchema::Primitive {
    name: "id".into(),
    r#type: PrimitiveType::Int64,
    nullable: false,
  }];
  let config = WriterConfig {
    column_configs: vec![ColumnConfig {
      column_name: "nonexistent".into(),
      compression: Some(Compression::Snappy),
      encoding: None,
      enable_dictionary: None,
    }],
    ..default_config()
  };
  let tmp = NamedTempFile::new().unwrap();
  let path = tmp.path().to_str().unwrap().to_string();
  let result = WriterHandle::new(path, schema, config);
  match result {
    Err(ParquetError::Schema { msg }) => {
      assert!(msg.contains("nonexistent"));
    }
    Err(other) => {
      panic!("expected ParquetError::Schema, got: {other:?}");
    }
    Ok(_) => {
      panic!("expected error but got Ok");
    }
  }
}

// ── Reader schema extraction ────────────────────────────────────────

#[test]
fn reader_returns_correct_schema() {
  let schema = vec![
    FieldSchema::Primitive {
      name: "id".into(),
      r#type: PrimitiveType::Int64,
      nullable: false,
    },
    FieldSchema::Primitive {
      name: "name".into(),
      r#type: PrimitiveType::Utf8,
      nullable: true,
    },
  ];
  let tmp = NamedTempFile::new().unwrap();
  let path = tmp.path().to_str().unwrap().to_string();

  let writer = WriterHandle::new(path.clone(), schema.clone(), default_config()).unwrap();
  writer
    .append_row(vec![
      ColumnValue::Int64 { v: 1 },
      ColumnValue::Utf8 { v: "test".into() },
    ])
    .unwrap();
  writer.close().unwrap();

  let reader = ReaderHandle::new(path).unwrap();
  let read_schema = reader.schema().unwrap();
  assert_eq!(read_schema, schema);
}

// ── Compression configs ─────────────────────────────────────────────

#[test]
fn round_trip_with_snappy() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Utf8,
    nullable: false,
  }];
  let config = WriterConfig {
    compression: Compression::Snappy,
    ..default_config()
  };
  let rows = vec![vec![ColumnValue::Utf8 {
    v: "compressed".into(),
  }]];
  let result = write_and_read_with_config(schema, rows.clone(), config);
  assert_eq!(result, rows);
}

#[test]
fn round_trip_with_zstd() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Utf8,
    nullable: false,
  }];
  let config = WriterConfig {
    compression: Compression::Zstd { level: 3 },
    ..default_config()
  };
  let rows = vec![vec![ColumnValue::Utf8 {
    v: "compressed".into(),
  }]];
  let result = write_and_read_with_config(schema, rows.clone(), config);
  assert_eq!(result, rows);
}

#[test]
fn round_trip_with_column_config() {
  let schema = vec![
    FieldSchema::Primitive {
      name: "id".into(),
      r#type: PrimitiveType::Int64,
      nullable: false,
    },
    FieldSchema::Primitive {
      name: "name".into(),
      r#type: PrimitiveType::Utf8,
      nullable: false,
    },
  ];
  let config = WriterConfig {
    column_configs: vec![ColumnConfig {
      column_name: "name".into(),
      compression: Some(Compression::Zstd { level: 3 }),
      encoding: None,
      enable_dictionary: Some(true),
    }],
    ..default_config()
  };
  let rows = vec![vec![
    ColumnValue::Int64 { v: 1 },
    ColumnValue::Utf8 { v: "Alice".into() },
  ]];
  let result = write_and_read_with_config(schema, rows.clone(), config);
  assert_eq!(result, rows);
}

// ── New type round-trips ────────────────────────────────────────────

#[test]
fn round_trip_time_ms() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimeMs,
    nullable: false,
  }];
  // TIME(MILLIS) uses int32: milliseconds since midnight
  let rows = vec![
    vec![ColumnValue::Int32 { v: 0 }],          // midnight
    vec![ColumnValue::Int32 { v: 43_200_000 }], // noon
    vec![ColumnValue::Int32 { v: 86_399_999 }], // 23:59:59.999
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_time_ns() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimeNs,
    nullable: false,
  }];
  // TIME(NANOS) uses int64: nanoseconds since midnight
  let rows = vec![
    vec![ColumnValue::Int64 { v: 0 }],
    vec![ColumnValue::Int64 {
      v: 43_200_000_000_000,
    }],
    vec![ColumnValue::Int64 {
      v: 86_399_999_999_999,
    }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_timestamp_ms_utc() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimestampMsUtc,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Int64 {
      v: 1_700_000_000_000,
    }],
    vec![ColumnValue::Int64 { v: 0 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_float16() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Float16,
    nullable: false,
  }];
  // Float16 values round-trip as f32; check they survive the f32→f16→f32 cycle
  let rows = vec![
    vec![ColumnValue::Float16 { v: 0.0 }],
    vec![ColumnValue::Float16 { v: 1.0 }],
    vec![ColumnValue::Float16 { v: -1.0 }],
    vec![ColumnValue::Float16 { v: 0.5 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_interval() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Interval,
    nullable: false,
  }];
  // Build a 12-byte little-endian payload: months=2, days=15, millis=3600000 (1 hour)
  let months: u32 = 2;
  let days: u32 = 15;
  let millis: u32 = 3_600_000;
  let mut payload = [0u8; 12];
  payload[0..4].copy_from_slice(&months.to_le_bytes());
  payload[4..8].copy_from_slice(&days.to_le_bytes());
  payload[8..12].copy_from_slice(&millis.to_le_bytes());
  let rows = vec![vec![ColumnValue::Bytes {
    v: payload.to_vec(),
  }]];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_map() {
  // MAP(utf8 -> int64): the schema has a Map variant
  let schema = vec![FieldSchema::Map {
    name: "labels".into(),
    fields: vec![
      FieldSchema::Primitive {
        name: "key".into(),
        r#type: PrimitiveType::Utf8,
        nullable: false,
      },
      FieldSchema::Primitive {
        name: "value".into(),
        r#type: PrimitiveType::Int64,
        nullable: true,
      },
    ],
    nullable: false,
  }];
  // Each row is a list of {key, value} struct entries
  let rows = vec![
    vec![ColumnValue::List {
      items: vec![
        ColumnValue::Struct {
          fields: vec![
            ColumnValue::Utf8 { v: "alpha".into() },
            ColumnValue::Int64 { v: 1 },
          ],
        },
        ColumnValue::Struct {
          fields: vec![
            ColumnValue::Utf8 { v: "beta".into() },
            ColumnValue::Int64 { v: 2 },
          ],
        },
      ],
    }],
    vec![ColumnValue::List { items: vec![] }], // empty map
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── NaN float round-trips ───────────────────────────────────────────
// NaN != NaN under IEEE 754 / PartialEq, so we can't use assert_eq!.

#[test]
fn round_trip_float32_nan() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Float32,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Float32 { v: f32::NAN }]];
  let result = write_and_read(schema, rows);
  let ColumnValue::Float32 { v } = &result[0][0] else {
    panic!("expected Float32");
  };
  assert!(v.is_nan(), "expected NaN, got {v}");
}

#[test]
fn round_trip_float64_nan() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Float64,
    nullable: false,
  }];
  let rows = vec![vec![ColumnValue::Float64 { v: f64::NAN }]];
  let result = write_and_read(schema, rows);
  let ColumnValue::Float64 { v } = &result[0][0] else {
    panic!("expected Float64");
  };
  assert!(v.is_nan(), "expected NaN, got {v}");
}

// ── Additional nullable variants ────────────────────────────────────

#[test]
fn round_trip_nullable_bool() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Bool,
    nullable: true,
  }];
  let rows = vec![
    vec![ColumnValue::Bool { v: true }],
    vec![ColumnValue::Null],
    vec![ColumnValue::Bool { v: false }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_nullable_utf8() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Utf8,
    nullable: true,
  }];
  let rows = vec![
    vec![ColumnValue::Utf8 { v: "hello".into() }],
    vec![ColumnValue::Null],
    vec![ColumnValue::Utf8 { v: "world".into() }],
    vec![ColumnValue::Null],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_nullable_float64() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Float64,
    nullable: true,
  }];
  let rows = vec![
    vec![ColumnValue::Float64 { v: 1.5 }],
    vec![ColumnValue::Null],
    vec![ColumnValue::Float64 { v: -99.0 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── Missing timestamp UTC variants ──────────────────────────────────

#[test]
fn round_trip_timestamp_us_utc() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimestampUsUtc,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Int64 {
      v: 1_700_000_000_000_000,
    }],
    vec![ColumnValue::Int64 { v: 0 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_timestamp_ns_utc() {
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::TimestampNsUtc,
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::Int64 {
      v: 1_700_000_000_000_000_000,
    }],
    vec![ColumnValue::Int64 { v: 0 }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── Missing collection coverage ─────────────────────────────────────

#[test]
fn round_trip_nested_list() {
  // [[String]]: a list whose elements are themselves lists of utf8
  let inner_schema = FieldSchema::List {
    name: "item".into(),
    fields: vec![FieldSchema::Primitive {
      name: "item".into(),
      r#type: PrimitiveType::Utf8,
      nullable: false,
    }],
    nullable: false,
  };
  let schema = vec![FieldSchema::List {
    name: "matrix".into(),
    fields: vec![inner_schema],
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::List {
      items: vec![
        ColumnValue::List {
          items: vec![
            ColumnValue::Utf8 { v: "a".into() },
            ColumnValue::Utf8 { v: "b".into() },
          ],
        },
        ColumnValue::List {
          items: vec![ColumnValue::Utf8 { v: "c".into() }],
        },
        ColumnValue::List { items: vec![] },
      ],
    }],
    vec![ColumnValue::List { items: vec![] }], // empty outer list
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_list_of_structs() {
  let struct_schema = FieldSchema::Struct {
    name: "item".into(),
    fields: vec![
      FieldSchema::Primitive {
        name: "key".into(),
        r#type: PrimitiveType::Utf8,
        nullable: false,
      },
      FieldSchema::Primitive {
        name: "val".into(),
        r#type: PrimitiveType::Int32,
        nullable: false,
      },
    ],
    nullable: false,
  };
  let schema = vec![FieldSchema::List {
    name: "items".into(),
    fields: vec![struct_schema],
    nullable: false,
  }];
  let rows = vec![
    vec![ColumnValue::List {
      items: vec![
        ColumnValue::Struct {
          fields: vec![
            ColumnValue::Utf8 { v: "x".into() },
            ColumnValue::Int32 { v: 1 },
          ],
        },
        ColumnValue::Struct {
          fields: vec![
            ColumnValue::Utf8 { v: "y".into() },
            ColumnValue::Int32 { v: 2 },
          ],
        },
      ],
    }],
    vec![ColumnValue::List { items: vec![] }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

#[test]
fn round_trip_nullable_struct() {
  // Struct column where nullable=true: some rows are Null, some are Struct.
  let schema = vec![FieldSchema::Struct {
    name: "info".into(),
    fields: vec![
      FieldSchema::Primitive {
        name: "key".into(),
        r#type: PrimitiveType::Utf8,
        nullable: false,
      },
      FieldSchema::Primitive {
        name: "val".into(),
        r#type: PrimitiveType::Int32,
        nullable: false,
      },
    ],
    nullable: true,
  }];
  let rows = vec![
    vec![ColumnValue::Struct {
      fields: vec![
        ColumnValue::Utf8 {
          v: "present".into(),
        },
        ColumnValue::Int32 { v: 42 },
      ],
    }],
    vec![ColumnValue::Null],
    vec![ColumnValue::Struct {
      fields: vec![
        ColumnValue::Utf8 {
          v: "also present".into(),
        },
        ColumnValue::Int32 { v: -1 },
      ],
    }],
  ];
  let result = write_and_read(schema, rows.clone());
  assert_eq!(result, rows);
}

// ── Error cases ─────────────────────────────────────────────────────

#[test]
fn io_error_nonexistent_file() {
  let result = ReaderHandle::new("/no/such/path_parquet_test.parquet".to_string());
  match result {
    Err(ParquetError::Io { .. }) => {}
    Err(other) => panic!("expected ParquetError::Io, got: {other}"),
    Ok(_) => panic!("expected an error but got Ok"),
  }
}

#[test]
fn invalid_file_error() {
  let tmp = tempfile::NamedTempFile::new().unwrap();
  std::fs::write(tmp.path(), b"this is not a parquet file").unwrap();
  let path = tmp.path().to_str().unwrap().to_string();
  let result = ReaderHandle::new(path);
  match result {
    Err(ParquetError::InvalidFile { .. }) => {}
    Err(ParquetError::Io { .. }) => {} // some backends surface corrupt files as Io
    Err(other) => panic!("expected InvalidFile or Io error, got: {other}"),
    Ok(_) => panic!("expected an error but got Ok"),
  }
}

#[test]
fn type_mismatch_writing_wrong_value() {
  // Schema says Int64 but we write Utf8; Rust value layer must reject it.
  let schema = vec![FieldSchema::Primitive {
    name: "v".into(),
    r#type: PrimitiveType::Int64,
    nullable: false,
  }];
  let tmp = tempfile::NamedTempFile::new().unwrap();
  let path = tmp.path().to_str().unwrap().to_string();
  let writer = WriterHandle::new(path, schema, default_config()).unwrap();
  let result = writer.append_row(vec![ColumnValue::Utf8 {
    v: "wrong type".into(),
  }]);
  match result {
    Err(ParquetError::TypeMismatch { .. }) => {}
    other => panic!("expected ParquetError::TypeMismatch, got: {other:?}"),
  }
}
