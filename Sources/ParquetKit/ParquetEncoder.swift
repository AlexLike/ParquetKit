// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import ParquetKitFFI

/// Schema-aware encoder that converts a `Codable` value into a `[ColumnValue]` row.
///
/// `ParquetEncoder` is constructed with a ``FieldSchema`` array and produces the
/// flat `ColumnValue` array consumed by the Rust writer handle.
/// It is used internally by ``ParquetWriter``.
///
/// Special types (`Decimal128`, `ParquetTimestamp`, `ParquetDate`, `ParquetTime`,
/// `Duration`, `UUID`) bypass the value's `Codable` implementation and are
/// converted directly to their Parquet wire representation using the schema.
public struct ParquetEncoder: Sendable {
  let schema: [FieldSchema]

  /// Creates an encoder for the given column schema.
  public init(schema: [FieldSchema]) {
    self.schema = schema
  }

  /// Encodes `value` into a flat row matching `schema`.
  ///
  /// - Throws: `EncodingError` if a value cannot be mapped to its schema type.
  public func encode<T: Encodable>(_ value: T) throws -> [ColumnValue] {
    let impl = EncoderImpl(schema: schema)
    try value.encode(to: impl)
    return impl.result()
  }
}

// MARK: - EncoderImpl

private final class EncoderImpl: Encoder, @unchecked Sendable {
  var codingPath: [CodingKey] = []
  var userInfo: [CodingUserInfoKey: Any] = [:]

  let schemaByName: [String: FieldSchema]
  let indexByName: [String: Int]
  private var values: [ColumnValue?]

  init(schema: [FieldSchema]) {
    var byName: [String: FieldSchema] = [:]
    var byIndex: [String: Int] = [:]
    for (i, field) in schema.enumerated() {
      let name = field.fieldName
      byName[name] = field
      byIndex[name] = i
    }
    self.schemaByName = byName
    self.indexByName = byIndex
    self.values = Array(repeating: nil, count: schema.count)
  }

  func result() -> [ColumnValue] {
    values.map { $0 ?? .null }
  }

  func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
    KeyedEncodingContainer(KeyedContainer<Key>(encoder: self))
  }

  func unkeyedContainer() -> UnkeyedEncodingContainer {
    fatalError("Top-level unkeyed container not supported")
  }

  func singleValueContainer() -> SingleValueEncodingContainer {
    fatalError("Top-level single value container not supported")
  }

  fileprivate func set(_ value: ColumnValue, forKey key: String) {
    guard let index = indexByName[key] else { return }
    values[index] = value
  }
}

// MARK: - KeyedContainer

private struct KeyedContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
  var codingPath: [CodingKey] = []
  let encoder: EncoderImpl

  mutating func encodeNil(forKey key: Key) throws {
    encoder.set(.null, forKey: key.stringValue)
  }

  mutating func encode(_ value: Bool, forKey key: Key) throws {
    encoder.set(.bool(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: String, forKey key: Key) throws {
    encoder.set(.utf8(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: Double, forKey key: Key) throws {
    encoder.set(.float64(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: Float, forKey key: Key) throws {
    // When Float16 is available it is the canonical Swift type for .float16 columns;
    // a bare Float should only be routed to float16 on older OS where Float16 does
    // not exist and Float is the only option.
    if #unavailable(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0),
      case .primitive(_, let ffiType, _) = encoder.schemaByName[key.stringValue]?._ffi,
      PrimitiveType(ffiType) == .float16
    {
      encoder.set(.float16(v: value), forKey: key.stringValue)
    } else {
      encoder.set(.float32(v: value), forKey: key.stringValue)
    }
  }
  mutating func encode(_ value: Int, forKey key: Key) throws {
    encoder.set(.int64(v: Int64(value)), forKey: key.stringValue)
  }
  mutating func encode(_ value: Int8, forKey key: Key) throws {
    encoder.set(.int8(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: Int16, forKey key: Key) throws {
    encoder.set(.int16(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: Int32, forKey key: Key) throws {
    encoder.set(.int32(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: Int64, forKey key: Key) throws {
    encoder.set(.int64(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: UInt, forKey key: Key) throws {
    encoder.set(.uInt64(v: UInt64(value)), forKey: key.stringValue)
  }
  mutating func encode(_ value: UInt8, forKey key: Key) throws {
    encoder.set(.uInt8(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: UInt16, forKey key: Key) throws {
    encoder.set(.uInt16(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: UInt32, forKey key: Key) throws {
    encoder.set(.uInt32(v: value), forKey: key.stringValue)
  }
  mutating func encode(_ value: UInt64, forKey key: Key) throws {
    encoder.set(.uInt64(v: value), forKey: key.stringValue)
  }

  mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
    let schema = encoder.schemaByName[key.stringValue]

    // Intercept special types based on schema.
    if let schema = schema {
      if let cv = try encodeSpecialType(value, schema: schema) {
        encoder.set(cv, forKey: key.stringValue)
        return
      }
    }

    // Handle MAP schema; encode Dictionary as a list-of-{key,value}-structs.
    if case .map(_, let ffiFields, _) = schema?._ffi, ffiFields.count == 2 {
      let keySchema = FieldSchema(ffiFields[0])
      let valSchema = FieldSchema(ffiFields[1])
      if let mapEnc = value as? any _ParquetMapEncodable {
        let cv = try mapEnc._encodeMapEntries(keySchema: keySchema, valueSchema: valSchema)
        encoder.set(cv, forKey: key.stringValue)
        return
      }
    }

    // For lists, encode via a nested unkeyed container.
    if case .list(_, let ffiFields, _) = schema?._ffi, let elementFFI = ffiFields.first {
      let elementSchema = FieldSchema(elementFFI)
      let container = ListEncodingContainer(
        codingPath: codingPath + [key],
        encoder: encoder,
        key: key.stringValue,
        elementSchema: elementSchema
      )
      let listEncoder = ListWrapperEncoder(container: container)
      try value.encode(to: listEncoder)
      encoder.set(.list(items: listEncoder.container.items), forKey: key.stringValue)
      return
    }

    // For nested Codable structs, check if the schema says it's a Struct.
    if case .struct(_, let ffiFields, _) = schema?._ffi {
      let fields = ffiFields.map(FieldSchema.init)
      let childEncoder = EncoderImpl(schema: fields)
      try value.encode(to: childEncoder)
      encoder.set(.struct(fields: childEncoder.result()), forKey: key.stringValue)
      return
    }

    // Try encoding as a primitive type.
    if let cv = encodePrimitive(value) {
      encoder.set(cv, forKey: key.stringValue)
      return
    }

    // Fallback: UUID as fixed bytes.
    if let uuid = value as? UUID {
      let data = withUnsafeBytes(of: uuid.uuid) { Data($0) }
      encoder.set(.bytes(v: data), forKey: key.stringValue)
      return
    }

    throw EncodingError.invalidValue(
      value,
      EncodingError.Context(
        codingPath: codingPath + [key],
        debugDescription: "Cannot encode \(type(of: value)) for key \(key.stringValue)"
      )
    )
  }

  mutating func nestedContainer<NestedKey: CodingKey>(
    keyedBy keyType: NestedKey.Type,
    forKey key: Key
  ) -> KeyedEncodingContainer<NestedKey> {
    fatalError("nestedContainer not supported in ParquetEncoder")
  }

  mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
    let schema = encoder.schemaByName[key.stringValue]
    if case .list(_, let ffiFields, _) = schema?._ffi, let elementFFI = ffiFields.first {
      return ListEncodingContainer(
        codingPath: codingPath + [key],
        encoder: encoder,
        key: key.stringValue,
        elementSchema: FieldSchema(elementFFI)
      )
    }
    fatalError("nestedUnkeyedContainer requires a List schema for key \(key.stringValue)")
  }

  mutating func superEncoder() -> Swift.Encoder {
    fatalError("superEncoder not supported")
  }
  mutating func superEncoder(forKey key: Key) -> Swift.Encoder {
    fatalError("superEncoder not supported")
  }
}

// MARK: - ListEncodingContainer

private class ListEncodingContainer: UnkeyedEncodingContainer, @unchecked Sendable {
  var codingPath: [CodingKey]
  var count: Int = 0
  let parentEncoder: EncoderImpl
  let key: String
  let elementSchema: FieldSchema
  var items: [ColumnValue] = []
  /// When set, deinit calls this instead of committing to parentEncoder.
  /// Used for nested lists returned from nestedUnkeyedContainer().
  /// Captured with [weak self] at the call site to avoid retain cycles.
  var nestedCommit: (([ColumnValue]) -> Void)?

  init(codingPath: [CodingKey], encoder: EncoderImpl, key: String, elementSchema: FieldSchema) {
    self.codingPath = codingPath
    self.parentEncoder = encoder
    self.key = key
    self.elementSchema = elementSchema
  }

  func encodeNil() throws {
    items.append(.null)
    count += 1
  }
  func encode(_ value: Bool) throws {
    items.append(.bool(v: value))
    count += 1
  }
  func encode(_ value: String) throws {
    items.append(.utf8(v: value))
    count += 1
  }
  func encode(_ value: Double) throws {
    items.append(.float64(v: value))
    count += 1
  }
  func encode(_ value: Float) throws {
    items.append(.float32(v: value))
    count += 1
  }
  func encode(_ value: Int) throws {
    items.append(.int64(v: Int64(value)))
    count += 1
  }
  func encode(_ value: Int8) throws {
    items.append(.int8(v: value))
    count += 1
  }
  func encode(_ value: Int16) throws {
    items.append(.int16(v: value))
    count += 1
  }
  func encode(_ value: Int32) throws {
    items.append(.int32(v: value))
    count += 1
  }
  func encode(_ value: Int64) throws {
    items.append(.int64(v: value))
    count += 1
  }
  func encode(_ value: UInt) throws {
    items.append(.uInt64(v: UInt64(value)))
    count += 1
  }
  func encode(_ value: UInt8) throws {
    items.append(.uInt8(v: value))
    count += 1
  }
  func encode(_ value: UInt16) throws {
    items.append(.uInt16(v: value))
    count += 1
  }
  func encode(_ value: UInt32) throws {
    items.append(.uInt32(v: value))
    count += 1
  }
  func encode(_ value: UInt64) throws {
    items.append(.uInt64(v: value))
    count += 1
  }

  func encode<T: Encodable>(_ value: T) throws {
    // Optional<Wrapped> elements: nil → .null, some → encode the wrapped value.
    if let optional = value as? any ParquetListOptional {
      try optional.encodeToList(self)
      return
    }
    if let cv = encodePrimitive(value) {
      items.append(cv)
      count += 1
      return
    }
    if let cv = try encodeSpecialType(value, schema: elementSchema) {
      items.append(cv)
      count += 1
      return
    }
    if case .struct(_, let ffiFields, _) = elementSchema._ffi {
      let fields = ffiFields.map(FieldSchema.init)
      let childEncoder = EncoderImpl(schema: fields)
      try value.encode(to: childEncoder)
      items.append(.struct(fields: childEncoder.result()))
      count += 1
      return
    }
    // List-of-list: element schema is itself a list; recurse into a nested container.
    // We use key "" so the deinit commit targets a nonexistent column and is a no-op.
    if case .list(_, let innerFFIFields, _) = elementSchema._ffi {
      let innerElementSchema = innerFFIFields.first.map(FieldSchema.init) ?? elementSchema
      let innerContainer = ListEncodingContainer(
        codingPath: codingPath,
        encoder: parentEncoder,
        key: "",
        elementSchema: innerElementSchema
      )
      let innerEncoder = ListWrapperEncoder(container: innerContainer)
      try value.encode(to: innerEncoder)
      items.append(.list(items: innerEncoder.container.items))
      count += 1
      return
    }
    throw EncodingError.invalidValue(
      value,
      EncodingError.Context(
        codingPath: codingPath,
        debugDescription: "Cannot encode \(type(of: value)) in list"
      )
    )
  }

  func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type)
    -> KeyedEncodingContainer<NestedKey>
  {
    fatalError("Nested keyed container in list not supported")
  }

  /// Returns a nested list container whose items are committed back to this container on deinit.
  /// Requires the element schema to be a `.list(...)`; otherwise crashes.
  func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
    guard case .list(_, let innerFFIFields, _) = elementSchema._ffi else {
      fatalError("nestedUnkeyedContainer called on a non-list element schema")
    }
    let innerSchema = innerFFIFields.first.map(FieldSchema.init) ?? elementSchema
    let inner = ListEncodingContainer(
      codingPath: codingPath,
      encoder: parentEncoder,
      key: "",  // dummy; nestedCommit overrides deinit behavior
      elementSchema: innerSchema
    )
    // [weak self] breaks the potential retain cycle: inner → nestedCommit → outer.
    // The outer container does NOT hold a strong reference to inner.
    inner.nestedCommit = { [weak self] innerItems in
      self?.items.append(.list(items: innerItems))
      self?.count += 1
    }
    return inner
  }

  func superEncoder() -> Swift.Encoder {
    fatalError("superEncoder not supported")
  }

  deinit {
    if let nestedCommit {
      // Nested list: append our collected items to the parent list container.
      nestedCommit(items)
    } else {
      // Top-level list: commit to the parent keyed encoder.
      // In the encode<T> path items are also committed manually before this fires;
      // the double-set is harmless (same value, or key "" → no-op).
      parentEncoder.set(.list(items: items), forKey: key)
    }
  }
}

// MARK: - ListWrapperEncoder

/// Wraps a `ListEncodingContainer` behind an `Encoder` interface so that
/// `Array.encode(to:)` can call `unkeyedContainer()`.
private class ListWrapperEncoder: Encoder, @unchecked Sendable {
  var codingPath: [CodingKey] = []
  var userInfo: [CodingUserInfoKey: Any] = [:]
  let container: ListEncodingContainer

  init(container: ListEncodingContainer) {
    self.container = container
  }

  func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
    fatalError("Array encoding requires unkeyed container")
  }
  func unkeyedContainer() -> UnkeyedEncodingContainer { container }
  func singleValueContainer() -> SingleValueEncodingContainer {
    fatalError("Array encoding requires unkeyed container")
  }
}

// MARK: - Optional List Element Encoding

/// Allows `Optional<Wrapped>` to encode itself into a `ListEncodingContainer`
/// by appending `.null` for `nil` and delegating to `encode(_:)` for `some`.
private protocol ParquetListOptional {
  func encodeToList(_ container: ListEncodingContainer) throws
}

extension Optional: ParquetListOptional where Wrapped: Encodable {
  fileprivate func encodeToList(_ container: ListEncodingContainer) throws {
    switch self {
    case .none:
      container.items.append(.null)
      container.count += 1
    case .some(let v):
      try container.encode(v)
    }
  }
}

// MARK: - Primitive Encoding

private func encodePrimitive<T>(_ value: T) -> ColumnValue? {
  switch value {
  case let v as Bool: return .bool(v: v)
  case let v as Int8: return .int8(v: v)
  case let v as Int16: return .int16(v: v)
  case let v as Int32: return .int32(v: v)
  case let v as Int64: return .int64(v: v)
  case let v as Int: return .int64(v: Int64(v))
  case let v as UInt8: return .uInt8(v: v)
  case let v as UInt16: return .uInt16(v: v)
  case let v as UInt32: return .uInt32(v: v)
  case let v as UInt64: return .uInt64(v: v)
  case let v as UInt: return .uInt64(v: UInt64(v))
  case let v as Float: return .float32(v: v)
  case let v as Double: return .float64(v: v)
  case let v as String: return .utf8(v: v)
  case let v as Data: return .bytes(v: v)
  default: return nil
  }
}

// MARK: - Special Type Encoding

/// Encodes a value as a special Parquet type guided by its `FieldSchema`.
/// Returns `nil` when the value/schema combination is not a recognised special type.
/// Throws when a `Duration` value cannot be encoded on the current OS.
private func encodeSpecialType<T: Encodable>(_ value: T, schema: FieldSchema) throws -> ColumnValue?
{
  guard case .primitive(_, let ffiType, _) = schema._ffi else { return nil }
  let pt = PrimitiveType(ffiType)
  switch pt {
  case .decimal128:
    if let d = value as? Decimal128 { return .bytes(v: Data(d.toBytes())) }
  case .timestampMs, .timestampUs, .timestampNs,
    .timestampMsUtc, .timestampUsUtc, .timestampNsUtc:
    if let ts = value as? ParquetTimestamp { return .int64(v: ts.toInt64()) }
  case .date32:
    if let d = value as? ParquetDate { return .int32(v: d.daysSinceEpoch) }
  case .timeMs:
    if let t = value as? ParquetTime { return .int32(v: Int32(t.valueSinceMidnight)) }
  case .timeUs:
    if let t = value as? ParquetTime { return .int64(v: t.valueSinceMidnight) }
  case .timeNs:
    if let t = value as? ParquetTime { return .int64(v: t.valueSinceMidnight) }
  case .interval:
    if let i = value as? ParquetInterval { return .bytes(v: i.toBytes()) }
  case .durationNs:
    if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *) {
      if let d = value as? Duration {
        let c = d.components
        return .int64(v: c.seconds * 1_000_000_000 + c.attoseconds / 1_000_000_000)
      }
    } else {
      throw EncodingError.invalidValue(
        value,
        EncodingError.Context(
          codingPath: [],
          debugDescription:
            "Encoding Duration requires macOS 13+, iOS 16+, tvOS 16+, or watchOS 9+."
        )
      )
    }
  case .float16:
    if #available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *) {
      if let v = value as? Float16 { return .float16(v: Float(v)) }
    } else {
      if let v = value as? Float { return .float16(v: v) }
    }
  case .fixedBytes:
    if let uuid = value as? UUID {
      return .bytes(v: withUnsafeBytes(of: uuid.uuid) { Data($0) })
    }
    if let data = value as? Data { return .bytes(v: data) }
  case .bytes:
    if let data = value as? Data { return .bytes(v: data) }
  default:
    break
  }
  return nil
}

// MARK: - Map Encoding Support

/// Private protocol that lets `Dictionary<K, V>` encode itself into a
/// `ColumnValue.list` of `ColumnValue.struct([key, value])` items when the
/// column schema is a Parquet MAP.
private protocol _ParquetMapEncodable {
  func _encodeMapEntries(keySchema: FieldSchema, valueSchema: FieldSchema) throws -> ColumnValue
}

extension Dictionary: _ParquetMapEncodable where Key: Encodable, Value: Encodable {
  fileprivate func _encodeMapEntries(
    keySchema: FieldSchema,
    valueSchema: FieldSchema
  ) throws -> ColumnValue {
    var items: [ColumnValue] = []
    items.reserveCapacity(count)
    for (k, v) in self {
      // Reuse existing free functions; map key/value types are always primitives.
      let keyCV =
        (try encodeSpecialType(k, schema: keySchema)) ?? encodePrimitive(k) ?? .null
      let valCV =
        (try encodeSpecialType(v, schema: valueSchema)) ?? encodePrimitive(v) ?? .null
      items.append(.struct(fields: [keyCV, valCV]))
    }
    return .list(items: items)
  }
}
