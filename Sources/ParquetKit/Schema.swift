// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import ParquetKitFFI

// MARK: - PrimitiveType

/// The primitive column type used in a Parquet schema.
///
/// Used as the `type` parameter of ``FieldSchema/primitive(name:type:nullable:)``.
public enum PrimitiveType: Sendable, Equatable, Hashable {
  case bool
  case int8, int16, int32, int64
  case uInt8, uInt16, uInt32, uInt64
  case float32, float64
  /// UTF-8 encoded string.
  case utf8
  /// Arbitrary byte sequence.
  case bytes
  /// Days since Unix epoch (Parquet `DATE`).
  case date32
  /// Milliseconds since midnight (Parquet `TIME(MILLIS)`, int32).
  case timeMs
  /// Microseconds since midnight (Parquet `TIME(MICROS)`, int64).
  case timeUs
  /// Nanoseconds since midnight (Parquet `TIME(NANOS)`, int64).
  case timeNs
  /// Milliseconds since Unix epoch (wall-clock).
  case timestampMs
  /// Microseconds since Unix epoch (wall-clock).
  case timestampUs
  /// Nanoseconds since Unix epoch (wall-clock).
  case timestampNs
  /// Milliseconds since Unix epoch (UTC-adjusted).
  case timestampMsUtc
  /// Microseconds since Unix epoch (UTC-adjusted).
  case timestampUsUtc
  /// Nanoseconds since Unix epoch (UTC-adjusted).
  case timestampNsUtc
  /// Nanosecond duration.
  case durationNs
  /// Parquet INTERVAL: months, days, and milliseconds as three independent `UInt32` values
  /// encoded in `FIXED_LEN_BYTE_ARRAY(12)` little-endian layout.
  case interval
  /// Half-precision (16-bit) floating-point.
  case float16
  /// UUID stored as `FIXED_LEN_BYTE_ARRAY(16)` with the Parquet UUID logical type.
  case uuid
  /// Fixed-length byte array of `size` bytes.
  case fixedBytes(size: UInt32)
  /// 128-bit decimal with explicit precision and scale.
  case decimal128(precision: UInt8, scale: UInt8)
}

// MARK: PrimitiveType ↔ FFI conversion (internal)

extension PrimitiveType {
  init(_ ffi: ParquetKitFFI.PrimitiveType) {
    switch ffi {
    case .bool: self = .bool
    case .int8: self = .int8
    case .int16: self = .int16
    case .int32: self = .int32
    case .int64: self = .int64
    case .uInt8: self = .uInt8
    case .uInt16: self = .uInt16
    case .uInt32: self = .uInt32
    case .uInt64: self = .uInt64
    case .float32: self = .float32
    case .float64: self = .float64
    case .utf8: self = .utf8
    case .bytes: self = .bytes
    case .date32: self = .date32
    case .timeMs: self = .timeMs
    case .timeUs: self = .timeUs
    case .timeNs: self = .timeNs
    case .timestampMs: self = .timestampMs
    case .timestampUs: self = .timestampUs
    case .timestampNs: self = .timestampNs
    case .timestampMsUtc: self = .timestampMsUtc
    case .timestampUsUtc: self = .timestampUsUtc
    case .timestampNsUtc: self = .timestampNsUtc
    case .durationNs: self = .durationNs
    case .interval: self = .interval
    case .float16: self = .float16
    case .uuid: self = .uuid
    case .fixedBytes(let size): self = .fixedBytes(size: size)
    case .decimal128(let p, let s): self = .decimal128(precision: p, scale: s)
    }
  }

  var ffi: ParquetKitFFI.PrimitiveType {
    switch self {
    case .bool: return .bool
    case .int8: return .int8
    case .int16: return .int16
    case .int32: return .int32
    case .int64: return .int64
    case .uInt8: return .uInt8
    case .uInt16: return .uInt16
    case .uInt32: return .uInt32
    case .uInt64: return .uInt64
    case .float32: return .float32
    case .float64: return .float64
    case .utf8: return .utf8
    case .bytes: return .bytes
    case .date32: return .date32
    case .timeMs: return .timeMs
    case .timeUs: return .timeUs
    case .timeNs: return .timeNs
    case .timestampMs: return .timestampMs
    case .timestampUs: return .timestampUs
    case .timestampNs: return .timestampNs
    case .timestampMsUtc: return .timestampMsUtc
    case .timestampUsUtc: return .timestampUsUtc
    case .timestampNsUtc: return .timestampNsUtc
    case .durationNs: return .durationNs
    case .interval: return .interval
    case .float16: return .float16
    case .uuid: return .uuid
    case .fixedBytes(let size): return .fixedBytes(size: size)
    case .decimal128(let p, let s): return .decimal128(precision: p, scale: s)
    }
  }
}

// MARK: - FieldSchema

/// Describes a Parquet column: its name, type, and nullability.
///
/// Construct a schema using the factory methods:
/// - ``primitive(name:type:nullable:)`` for scalar columns
/// - ``list(name:element:nullable:)`` for repeated columns
/// - ``structType(name:fields:nullable:)`` for nested struct columns
///
/// The ``Parquet()`` macro generates a `parquetSchema` implementation
/// automatically; use these methods when implementing ``ParquetCodable``
/// manually.
///
/// @Snippet(path: "ParquetKit/Snippets/FieldSchemaManualUsage")
public struct FieldSchema: Sendable, Equatable, Hashable {
  /// Internal FFI storage; not accessible to library consumers.
  let _ffi: ParquetKitFFI.FieldSchema

  init(_ ffi: ParquetKitFFI.FieldSchema) {
    self._ffi = ffi
  }

  // MARK: Factory methods

  /// A scalar column.
  public static func primitive(
    name: String,
    type: PrimitiveType,
    nullable: Bool
  ) -> FieldSchema {
    FieldSchema(.primitive(name: name, type: type.ffi, nullable: nullable))
  }

  /// A repeated (array) column.
  ///
  /// - Parameters:
  ///   - name: Column name.
  ///   - element: Schema of each list element.
  ///   - nullable: Whether the list itself can be null (not the elements).
  public static func list(
    name: String,
    element: FieldSchema,
    nullable: Bool
  ) -> FieldSchema {
    FieldSchema(.list(name: name, fields: [element._ffi], nullable: nullable))
  }

  /// A nested struct column.
  ///
  /// > Tip: The nested type should itself conform to ``ParquetCodable`` and
  /// > be decorated with ``Parquet()``.  Pass `NestedType.parquetSchema` as
  /// > the `fields` argument.
  ///
  /// @Snippet(path: "ParquetKit/Snippets/FieldSchemaStructUsage")
  ///
  /// - Parameters:
  ///   - name: Column name.
  ///   - fields: Schemas for the struct's child fields, in declaration order.
  ///   - nullable: Whether the struct itself can be null.
  public static func structType(
    name: String,
    fields: [FieldSchema],
    nullable: Bool
  ) -> FieldSchema {
    FieldSchema(.struct(name: name, fields: fields.map(\.ffiValue), nullable: nullable))
  }

  /// A Parquet MAP column (key→value dictionary).
  ///
  /// The ``Parquet()`` macro does not support `Dictionary` fields automatically;
  /// implement `parquetSchema` manually and use this factory to describe the column.
  ///
  /// @Snippet(path: "ParquetKit/Snippets/FieldSchemaMapUsage")
  ///
  /// - Parameters:
  ///   - name: Column name.
  ///   - keyType: Schema for the map key (must be a primitive type; keys are never null).
  ///   - valueType: Schema for the map value.
  ///   - nullable: Whether the map column itself can be null.
  public static func map(
    name: String,
    keyType: FieldSchema,
    valueType: FieldSchema,
    nullable: Bool
  ) -> FieldSchema {
    FieldSchema(.map(name: name, fields: [keyType._ffi, valueType._ffi], nullable: nullable))
  }

  // MARK: Public properties

  /// The column name as it appears in the Parquet file.
  public var fieldName: String {
    switch _ffi {
    case .primitive(let name, _, _): return name
    case .list(let name, _, _): return name
    case .struct(let name, _, _): return name
    case .map(let name, _, _): return name
    }
  }

  /// Whether this column allows null values.
  public var isNullable: Bool {
    switch _ffi {
    case .primitive(_, _, let nullable): return nullable
    case .list(_, _, let nullable): return nullable
    case .struct(_, _, let nullable): return nullable
    case .map(_, _, let nullable): return nullable
    }
  }

  /// The primitive type, or `nil` if this is not a primitive column.
  public var primitiveType: PrimitiveType? {
    guard case .primitive(_, let t, _) = _ffi else { return nil }
    return PrimitiveType(t)
  }

  /// The element schema for a list column, or `nil` if this is not a list.
  public var listElement: FieldSchema? {
    guard case .list(_, let fields, _) = _ffi else { return nil }
    return fields.first.map(FieldSchema.init)
  }

  /// The child field schemas for a struct column, or `nil` if not a struct.
  public var structFields: [FieldSchema]? {
    guard case .struct(_, let fields, _) = _ffi else { return nil }
    return fields.map(FieldSchema.init)
  }

  /// The key type schema for a map column, or `nil` if not a map.
  public var mapKeyType: FieldSchema? {
    guard case .map(_, let fields, _) = _ffi, fields.count == 2 else { return nil }
    return FieldSchema(fields[0])
  }

  /// The value type schema for a map column, or `nil` if not a map.
  public var mapValueType: FieldSchema? {
    guard case .map(_, let fields, _) = _ffi, fields.count == 2 else { return nil }
    return FieldSchema(fields[1])
  }
}

// MARK: Internal FFI access

extension FieldSchema {
  /// The raw FFI value; for use inside the ParquetKit module only.
  var ffiValue: ParquetKitFFI.FieldSchema { _ffi }
}

extension Array where Element == FieldSchema {
  /// Converts a Swift FieldSchema array to the FFI representation.
  var ffiValues: [ParquetKitFFI.FieldSchema] { map(\.ffiValue) }
}
