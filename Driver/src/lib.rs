// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(unsafe_code)]

mod reader;
mod schema;
mod value;
mod writer;

uniffi::setup_scaffolding!();

// ── Error ──────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum ParquetError {
  #[error("I/O error: {msg}")]
  Io { msg: String },
  #[error("Schema error: {msg}")]
  Schema { msg: String },
  #[error("Type mismatch: {msg}")]
  TypeMismatch { msg: String },
  #[error("Invalid file: {msg}")]
  InvalidFile { msg: String },
}

// ── Primitive types ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum PrimitiveType {
  Bool,
  Int8,
  Int16,
  Int32,
  Int64,
  UInt8,
  UInt16,
  UInt32,
  UInt64,
  Float32,
  Float64,
  Utf8,
  Bytes,
  Date32,
  TimeMs,
  TimeUs,
  TimeNs,
  TimestampMs,
  TimestampUs,
  TimestampNs,
  TimestampMsUtc,
  TimestampUsUtc,
  TimestampNsUtc,
  DurationNs,
  /// Parquet INTERVAL: FIXED_LEN_BYTE_ARRAY(12) encoding months (u32), days (u32),
  /// milliseconds (u32) in little-endian order.  Mapped to Arrow
  /// `Interval(MonthDayNano)` with millis converted to nanoseconds.
  Interval,
  Float16,
  Uuid,
  FixedBytes {
    size: u32,
  },
  Decimal128 {
    precision: u8,
    scale: u8,
  },
}

// ── Field schema (recursive) ──────────────────────────────────────────

/// Field schema for Parquet columns.
///
/// For the `List` variant, `fields` contains exactly one element: the element schema.
/// For the `Struct` variant, `fields` contains the struct's child field schemas.
/// For the `Primitive` variant, `fields` is empty.
#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum FieldSchema {
  Primitive {
    name: String,
    r#type: PrimitiveType,
    nullable: bool,
  },
  List {
    name: String,
    /// Exactly one element: the list's element schema.
    fields: Vec<FieldSchema>,
    nullable: bool,
  },
  Struct {
    name: String,
    fields: Vec<FieldSchema>,
    nullable: bool,
  },
  /// A Parquet MAP column.  `fields` contains exactly two elements: `[0]` is
  /// the key schema and `[1]` is the value schema (mirrors `List` convention).
  Map {
    name: String,
    fields: Vec<FieldSchema>,
    nullable: bool,
  },
}

// ── Compression & encoding ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum Compression {
  None,
  Snappy,
  Lz4,
  Gzip { level: u8 },
  Zstd { level: u8 },
  Brotli { level: u8 },
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum Encoding {
  Plain,
  DeltaBinaryPacked,
  DeltaLengthByteArray,
  DeltaByteArray,
  RleDictionary,
  ByteStreamSplit,
}

// ── Writer configuration ───────────────────────────────────────────────

#[derive(Debug, Clone, uniffi::Record)]
pub struct ColumnConfig {
  pub column_name: String,
  pub compression: Option<Compression>,
  pub encoding: Option<Encoding>,
  pub enable_dictionary: Option<bool>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct WriterConfig {
  pub compression: Compression,
  pub row_group_size: u32,
  pub data_page_size: u32,
  pub enable_dictionary: bool,
  pub enable_statistics: bool,
  pub column_configs: Vec<ColumnConfig>,
}

// ── Column values ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum ColumnValue {
  Null,
  Bool { v: bool },
  Int8 { v: i8 },
  Int16 { v: i16 },
  Int32 { v: i32 },
  Int64 { v: i64 },
  UInt8 { v: u8 },
  UInt16 { v: u16 },
  UInt32 { v: u32 },
  UInt64 { v: u64 },
  Float16 { v: f32 },
  Float32 { v: f32 },
  Float64 { v: f64 },
  Bytes { v: Vec<u8> },
  Utf8 { v: String },
  List { items: Vec<ColumnValue> },
  Struct { fields: Vec<ColumnValue> },
}

// Re-export handle types
pub use reader::ReaderHandle;
pub use writer::WriterHandle;
