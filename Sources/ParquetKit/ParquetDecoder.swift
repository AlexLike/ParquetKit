// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import ParquetKitFFI

/// Schema-aware decoder that converts a `[ColumnValue]` row into a `Decodable` value.
///
/// `ParquetDecoder` is the inverse of ``ParquetEncoder``.  It is constructed with
/// a ``FieldSchema`` array and the flat column values read from the Rust reader handle.
/// It is used internally by ``ParquetReader``.
///
/// Special types (`Decimal128`, `ParquetTimestamp`, `ParquetDate`, `ParquetTime`,
/// `Duration`, `UUID`) are reconstructed from their Parquet wire representation
/// using the schema.
public struct ParquetDecoder: Sendable {
  let schema: [FieldSchema]

  /// Creates a decoder for the given column schema.
  public init(schema: [FieldSchema]) {
    self.schema = schema
  }

  /// Decodes `values` into an instance of `type`.
  ///
  /// - Throws: `DecodingError` if a column value cannot be converted to the expected type.
  public func decode<T: Decodable>(_ type: T.Type, from values: [ColumnValue]) throws -> T {
    let impl = DecoderImpl(values: values, schema: schema)
    return try T(from: impl)
  }
}

// MARK: - DecoderImpl

private final class DecoderImpl: Decoder, @unchecked Sendable {
  var codingPath: [CodingKey] = []
  var userInfo: [CodingUserInfoKey: Any] = [:]

  let values: [ColumnValue]
  let schemaByName: [String: FieldSchema]
  let indexByName: [String: Int]

  init(values: [ColumnValue], schema: [FieldSchema]) {
    self.values = values
    var byName: [String: FieldSchema] = [:]
    var byIndex: [String: Int] = [:]
    for (i, field) in schema.enumerated() {
      let name = field.fieldName
      byName[name] = field
      byIndex[name] = i
    }
    self.schemaByName = byName
    self.indexByName = byIndex
  }

  func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
    KeyedDecodingContainer(KeyedContainer<Key>(decoder: self))
  }
  func unkeyedContainer() throws -> UnkeyedDecodingContainer {
    fatalError("Top-level unkeyed container not supported")
  }
  func singleValueContainer() throws -> SingleValueDecodingContainer {
    fatalError("Top-level single value container not supported")
  }
}

// MARK: - KeyedContainer

private struct KeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
  var codingPath: [CodingKey] = []
  var allKeys: [Key] { decoder.indexByName.keys.compactMap { Key(stringValue: $0) } }
  let decoder: DecoderImpl

  func contains(_ key: Key) -> Bool { decoder.indexByName[key.stringValue] != nil }

  private func value(forKey key: Key) throws -> ColumnValue {
    guard let index = decoder.indexByName[key.stringValue] else {
      throw DecodingError.keyNotFound(
        key,
        DecodingError.Context(
          codingPath: codingPath, debugDescription: "No value for key \(key.stringValue)")
      )
    }
    return decoder.values[index]
  }

  func decodeNil(forKey key: Key) throws -> Bool {
    guard let index = decoder.indexByName[key.stringValue] else { return true }
    return decoder.values[index] == .null
  }

  func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
    guard case .bool(let v) = try value(forKey: key) else {
      throw typeMismatch(Bool.self, forKey: key)
    }
    return v
  }
  func decode(_ type: String.Type, forKey key: Key) throws -> String {
    guard case .utf8(let v) = try value(forKey: key) else {
      throw typeMismatch(String.self, forKey: key)
    }
    return v
  }
  func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
    guard case .float64(let v) = try value(forKey: key) else {
      throw typeMismatch(Double.self, forKey: key)
    }
    return v
  }
  func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
    let cv = try value(forKey: key)
    if case .float32(let v) = cv { return v }
    // Fall back to float16 → Float only when native Float16 type is unavailable.
    if #unavailable(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0) {
      if case .float16(let v) = cv { return v }
    }
    throw typeMismatch(Float.self, forKey: key)
  }
  func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
    guard case .int64(let v) = try value(forKey: key) else {
      throw typeMismatch(Int.self, forKey: key)
    }
    return Int(v)
  }
  func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
    guard case .int8(let v) = try value(forKey: key) else {
      throw typeMismatch(Int8.self, forKey: key)
    }
    return v
  }
  func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
    guard case .int16(let v) = try value(forKey: key) else {
      throw typeMismatch(Int16.self, forKey: key)
    }
    return v
  }
  func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
    guard case .int32(let v) = try value(forKey: key) else {
      throw typeMismatch(Int32.self, forKey: key)
    }
    return v
  }
  func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
    guard case .int64(let v) = try value(forKey: key) else {
      throw typeMismatch(Int64.self, forKey: key)
    }
    return v
  }
  func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
    guard case .uInt64(let v) = try value(forKey: key) else {
      throw typeMismatch(UInt.self, forKey: key)
    }
    return UInt(v)
  }
  func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
    guard case .uInt8(let v) = try value(forKey: key) else {
      throw typeMismatch(UInt8.self, forKey: key)
    }
    return v
  }
  func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
    guard case .uInt16(let v) = try value(forKey: key) else {
      throw typeMismatch(UInt16.self, forKey: key)
    }
    return v
  }
  func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
    guard case .uInt32(let v) = try value(forKey: key) else {
      throw typeMismatch(UInt32.self, forKey: key)
    }
    return v
  }
  func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
    guard case .uInt64(let v) = try value(forKey: key) else {
      throw typeMismatch(UInt64.self, forKey: key)
    }
    return v
  }

  func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
    let cv = try value(forKey: key)
    let schema = decoder.schemaByName[key.stringValue]

    if let schema {
      if let result: T = try decodeSpecialType(cv, schema: schema) { return result }
    }

    // List
    if case .list(_, let ffiFields, _) = schema?._ffi,
      case .list(let items) = cv
    {
      let elementSchema = ffiFields.first.map(FieldSchema.init)
      let listDecoder = ListWrapperDecoder(
        items: items, elementSchema: elementSchema, codingPath: codingPath + [key])
      return try T(from: listDecoder)
    }

    // Nested struct
    if case .struct(_, let ffiFields, _) = schema?._ffi,
      case .struct(let childValues) = cv
    {
      let fields = ffiFields.map(FieldSchema.init)
      let childDecoder = DecoderImpl(values: childValues, schema: fields)
      return try T(from: childDecoder)
    }

    // MAP: decode list-of-{key,value}-structs into a Dictionary.
    if case .map(_, let ffiFields, _) = schema?._ffi, ffiFields.count == 2,
      case .list(let items) = cv
    {
      let keySchema = FieldSchema(ffiFields[0])
      let valSchema = FieldSchema(ffiFields[1])
      if let mapDec = type as? any _ParquetMapDecodable.Type {
        return try mapDec._decodeMapEntries(items, keySchema: keySchema, valueSchema: valSchema)
          as! T
      }
    }

    // Data
    if type == Data.self, case .bytes(let v) = cv { return v as! T }

    // UUID
    if type == UUID.self, case .bytes(let v) = cv, v.count == 16 {
      let uuid = v.withUnsafeBytes { ptr -> UUID in
        ptr.baseAddress!.withMemoryRebound(to: uuid_t.self, capacity: 1) { UUID(uuid: $0.pointee) }
      }
      return uuid as! T
    }

    throw DecodingError.typeMismatch(
      type,
      DecodingError.Context(
        codingPath: codingPath + [key], debugDescription: "Cannot decode \(type) from \(cv)")
    )
  }

  func nestedContainer<NestedKey: CodingKey>(
    keyedBy type: NestedKey.Type, forKey key: Key
  ) throws -> KeyedDecodingContainer<NestedKey> {
    fatalError("nestedContainer not supported in ParquetDecoder")
  }
  func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
    let cv = try value(forKey: key)
    guard case .list(let items) = cv else { throw typeMismatch([Any].self, forKey: key) }
    let schema = decoder.schemaByName[key.stringValue]
    let elementSchema: FieldSchema?
    if case .list(_, let ffiFields, _) = schema?._ffi {
      elementSchema = ffiFields.first.map(FieldSchema.init)
    } else {
      elementSchema = nil
    }
    return ListDecodingContainer(
      codingPath: codingPath + [key], items: items, elementSchema: elementSchema)
  }
  func superDecoder() throws -> Swift.Decoder { fatalError("superDecoder not supported") }
  func superDecoder(forKey key: Key) throws -> Swift.Decoder {
    fatalError("superDecoder not supported")
  }

  private func typeMismatch(_ type: Any.Type, forKey key: Key) -> DecodingError {
    let cv = (try? value(forKey: key)).map { "\($0)" } ?? "missing"
    return DecodingError.typeMismatch(
      type,
      DecodingError.Context(
        codingPath: codingPath + [key], debugDescription: "Expected \(type) but found \(cv)")
    )
  }
}

// MARK: - ListDecodingContainer

private struct ListDecodingContainer: UnkeyedDecodingContainer {
  var codingPath: [CodingKey]
  var count: Int?
  var isAtEnd: Bool { currentIndex >= items.count }
  var currentIndex: Int = 0
  let items: [ColumnValue]
  let elementSchema: FieldSchema?

  init(codingPath: [CodingKey], items: [ColumnValue], elementSchema: FieldSchema?) {
    self.codingPath = codingPath
    self.items = items
    self.count = items.count
    self.elementSchema = elementSchema
  }

  private mutating func next() -> ColumnValue {
    let v = items[currentIndex]
    currentIndex += 1
    return v
  }

  mutating func decodeNil() throws -> Bool {
    if items[currentIndex] == .null {
      currentIndex += 1
      return true
    }
    return false
  }
  mutating func decode(_ type: Bool.Type) throws -> Bool {
    guard case .bool(let v) = next() else { throw listTypeMismatch(Bool.self) }
    return v
  }
  mutating func decode(_ type: String.Type) throws -> String {
    guard case .utf8(let v) = next() else { throw listTypeMismatch(String.self) }
    return v
  }
  mutating func decode(_ type: Double.Type) throws -> Double {
    guard case .float64(let v) = next() else { throw listTypeMismatch(Double.self) }
    return v
  }
  mutating func decode(_ type: Float.Type) throws -> Float {
    guard case .float32(let v) = next() else { throw listTypeMismatch(Float.self) }
    return v
  }
  mutating func decode(_ type: Int.Type) throws -> Int {
    guard case .int64(let v) = next() else { throw listTypeMismatch(Int.self) }
    return Int(v)
  }
  mutating func decode(_ type: Int8.Type) throws -> Int8 {
    guard case .int8(let v) = next() else { throw listTypeMismatch(Int8.self) }
    return v
  }
  mutating func decode(_ type: Int16.Type) throws -> Int16 {
    guard case .int16(let v) = next() else { throw listTypeMismatch(Int16.self) }
    return v
  }
  mutating func decode(_ type: Int32.Type) throws -> Int32 {
    guard case .int32(let v) = next() else { throw listTypeMismatch(Int32.self) }
    return v
  }
  mutating func decode(_ type: Int64.Type) throws -> Int64 {
    guard case .int64(let v) = next() else { throw listTypeMismatch(Int64.self) }
    return v
  }
  mutating func decode(_ type: UInt.Type) throws -> UInt {
    guard case .uInt64(let v) = next() else { throw listTypeMismatch(UInt.self) }
    return UInt(v)
  }
  mutating func decode(_ type: UInt8.Type) throws -> UInt8 {
    guard case .uInt8(let v) = next() else { throw listTypeMismatch(UInt8.self) }
    return v
  }
  mutating func decode(_ type: UInt16.Type) throws -> UInt16 {
    guard case .uInt16(let v) = next() else { throw listTypeMismatch(UInt16.self) }
    return v
  }
  mutating func decode(_ type: UInt32.Type) throws -> UInt32 {
    guard case .uInt32(let v) = next() else { throw listTypeMismatch(UInt32.self) }
    return v
  }
  mutating func decode(_ type: UInt64.Type) throws -> UInt64 {
    guard case .uInt64(let v) = next() else { throw listTypeMismatch(UInt64.self) }
    return v
  }

  mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
    let cv = next()
    // Optional<Wrapped> elements: .null → nil, otherwise decode the wrapped type.
    if let optType = type as? any ParquetListOptionalDecodable.Type {
      return try optType.decodeListElement(cv, elementSchema: elementSchema) as! T
    }
    if let result = decodePrimitive(cv, as: type) { return result }
    if let elementSchema {
      if let result: T = try decodeSpecialType(cv, schema: elementSchema) { return result }
      if case .struct(_, let ffiFields, _) = elementSchema._ffi,
        case .struct(let childValues) = cv
      {
        let fields = ffiFields.map(FieldSchema.init)
        let childDecoder = DecoderImpl(values: childValues, schema: fields)
        return try T(from: childDecoder)
      }
      // List-of-list: element schema is itself a list; recurse via ListWrapperDecoder.
      if case .list(_, let innerFFIFields, _) = elementSchema._ffi,
        case .list(let innerItems) = cv
      {
        let innerElementSchema = innerFFIFields.first.map(FieldSchema.init)
        let innerDecoder = ListWrapperDecoder(
          items: innerItems,
          elementSchema: innerElementSchema,
          codingPath: codingPath
        )
        return try T(from: innerDecoder)
      }
    }
    throw DecodingError.typeMismatch(
      type,
      DecodingError.Context(
        codingPath: codingPath, debugDescription: "Cannot decode \(type) from list element")
    )
  }

  mutating func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type) throws
    -> KeyedDecodingContainer<NestedKey>
  {
    fatalError("Nested keyed container in list not supported")
  }
  /// Returns a nested list container for the current element.
  /// Requires the current column value to be `.list(...)`; otherwise throws.
  mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
    let cv = next()
    guard case .list(let innerItems) = cv else {
      throw DecodingError.typeMismatch(
        [Any].self,
        DecodingError.Context(
          codingPath: codingPath, debugDescription: "Expected a nested list but found \(cv)")
      )
    }
    let innerElementSchema: FieldSchema?
    if let elementSchema, case .list(_, let innerFFIFields, _) = elementSchema._ffi {
      innerElementSchema = innerFFIFields.first.map(FieldSchema.init)
    } else {
      innerElementSchema = nil
    }
    return ListDecodingContainer(
      codingPath: codingPath, items: innerItems, elementSchema: innerElementSchema)
  }
  mutating func superDecoder() throws -> Swift.Decoder { fatalError("superDecoder not supported") }

  private func listTypeMismatch(_ type: Any.Type) -> DecodingError {
    DecodingError.typeMismatch(
      type,
      DecodingError.Context(
        codingPath: codingPath,
        debugDescription: "Type mismatch in list at index \(currentIndex - 1)")
    )
  }
}

// MARK: - Primitive Decoding

private func decodePrimitive<T>(_ cv: ColumnValue, as type: T.Type) -> T? {
  switch cv {
  case .bool(let v): return v as? T
  case .int8(let v): return v as? T
  case .int16(let v): return v as? T
  case .int32(let v): return v as? T
  case .int64(let v): return (type == Int.self ? Int(v) as? T : v as? T)
  case .uInt8(let v): return v as? T
  case .uInt16(let v): return v as? T
  case .uInt32(let v): return v as? T
  case .uInt64(let v): return (type == UInt.self ? UInt(v) as? T : v as? T)
  case .float32(let v): return v as? T
  case .float64(let v): return v as? T
  case .utf8(let v): return v as? T
  case .bytes(let v): return (type == Data.self ? v as? T : nil)
  default: return nil
  }
}

// MARK: - ListWrapperDecoder

/// Wraps a list of `ColumnValue`s behind a `Decoder` interface so that
/// `Array.init(from:)` can call `unkeyedContainer()`.
private class ListWrapperDecoder: Decoder, @unchecked Sendable {
  var codingPath: [CodingKey]
  var userInfo: [CodingUserInfoKey: Any] = [:]
  let items: [ColumnValue]
  let elementSchema: FieldSchema?

  init(items: [ColumnValue], elementSchema: FieldSchema?, codingPath: [CodingKey]) {
    self.items = items
    self.elementSchema = elementSchema
    self.codingPath = codingPath
  }

  func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
    throw DecodingError.typeMismatch(
      KeyedDecodingContainer<Key>.self,
      DecodingError.Context(
        codingPath: codingPath, debugDescription: "Expected unkeyed container for list")
    )
  }
  func unkeyedContainer() throws -> UnkeyedDecodingContainer {
    ListDecodingContainer(codingPath: codingPath, items: items, elementSchema: elementSchema)
  }
  func singleValueContainer() throws -> SingleValueDecodingContainer {
    throw DecodingError.typeMismatch(
      SingleValueDecodingContainer.self,
      DecodingError.Context(
        codingPath: codingPath, debugDescription: "Expected unkeyed container for list")
    )
  }
}

// MARK: - Special Type Decoding

private func decodeSpecialType<T: Decodable>(_ cv: ColumnValue, schema: FieldSchema) throws -> T? {
  guard case .primitive(_, let ffiType, _) = schema._ffi else { return nil }
  let pt = PrimitiveType(ffiType)
  switch pt {
  case .decimal128:
    if T.self == Decimal128.self, case .bytes(let v) = cv {
      return Decimal128.fromBytes(Array(v)) as? T
    }
  case .timestampMs:
    if T.self == ParquetTimestamp.self, case .int64(let v) = cv {
      return ParquetTimestamp.fromInt64(v, unit: .milliseconds) as? T
    }
  case .timestampUs:
    if T.self == ParquetTimestamp.self, case .int64(let v) = cv {
      return ParquetTimestamp.fromInt64(v, unit: .microseconds) as? T
    }
  case .timestampNs:
    if T.self == ParquetTimestamp.self, case .int64(let v) = cv {
      return ParquetTimestamp.fromInt64(v, unit: .nanoseconds) as? T
    }
  case .timestampMsUtc:
    if T.self == ParquetTimestamp.self, case .int64(let v) = cv {
      return ParquetTimestamp(
        date: ParquetTimestamp.fromInt64(v, unit: .milliseconds).date,
        unit: .milliseconds, isAdjustedToUTC: true) as? T
    }
  case .timestampUsUtc:
    if T.self == ParquetTimestamp.self, case .int64(let v) = cv {
      return ParquetTimestamp(
        date: ParquetTimestamp.fromInt64(v, unit: .microseconds).date,
        unit: .microseconds, isAdjustedToUTC: true) as? T
    }
  case .timestampNsUtc:
    if T.self == ParquetTimestamp.self, case .int64(let v) = cv {
      return ParquetTimestamp(
        date: ParquetTimestamp.fromInt64(v, unit: .nanoseconds).date,
        unit: .nanoseconds, isAdjustedToUTC: true) as? T
    }
  case .date32:
    if T.self == ParquetDate.self, case .int32(let v) = cv {
      return ParquetDate(daysSinceEpoch: v) as? T
    }
  case .timeMs:
    if T.self == ParquetTime.self, case .int32(let v) = cv {
      return ParquetTime(millisecondsSinceMidnight: Int64(v)) as? T
    }
  case .timeUs:
    if T.self == ParquetTime.self, case .int64(let v) = cv {
      return ParquetTime(microsecondsSinceMidnight: v) as? T
    }
  case .timeNs:
    if T.self == ParquetTime.self, case .int64(let v) = cv {
      return ParquetTime(nanosecondsSinceMidnight: v) as? T
    }
  case .float16:
    if #available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *) {
      if T.self == Float16.self, case .float16(let v) = cv { return Float16(v) as? T }
    } else {
      if T.self == Float.self, case .float16(let v) = cv { return v as? T }
    }
  case .interval:
    if T.self == ParquetInterval.self, case .bytes(let v) = cv, v.count == 12 {
      return ParquetInterval.fromBytes(v) as? T
    }
  case .durationNs:
    if case .int64(let v) = cv {
      if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *) {
        if T.self == Duration.self {
          let seconds = v / 1_000_000_000
          let nanos = v % 1_000_000_000
          return (Duration.seconds(seconds) + Duration.nanoseconds(nanos)) as? T
        }
      } else {
        throw DecodingError.typeMismatch(
          T.self,
          DecodingError.Context(
            codingPath: [],
            debugDescription:
              "Decoding Duration requires macOS 13+, iOS 16+, tvOS 16+, or watchOS 9+."
          )
        )
      }
    }
  case .uuid:
    // New schema type: FIXED_LEN_BYTE_ARRAY(16) with Parquet UUID logical type.
    if T.self == UUID.self, case .bytes(let v) = cv, v.count == 16 {
      let uuid = v.withUnsafeBytes { ptr -> UUID in
        ptr.baseAddress!.withMemoryRebound(to: uuid_t.self, capacity: 1) { UUID(uuid: $0.pointee) }
      }
      return uuid as? T
    }
  case .fixedBytes:
    // Legacy path: FIXED_LEN_BYTE_ARRAY(16) without UUID annotation.  Files
    // written by older versions of ParquetKit (or other tools that omit the
    // UUID logical type) still decode correctly here.
    if T.self == UUID.self, case .bytes(let v) = cv, v.count == 16 {
      let uuid = v.withUnsafeBytes { ptr -> UUID in
        ptr.baseAddress!.withMemoryRebound(to: uuid_t.self, capacity: 1) { UUID(uuid: $0.pointee) }
      }
      return uuid as? T
    }
  default:
    break
  }
  return nil
}

// MARK: - Optional List Element Decoding

/// Allows `Optional<Wrapped>` to decode itself from a `ColumnValue` in a list context,
/// returning `nil` for `.null` and the decoded wrapped value otherwise.
private protocol ParquetListOptionalDecodable {
  static func decodeListElement(_ cv: ColumnValue, elementSchema: FieldSchema?) throws -> Self
}

extension Optional: ParquetListOptionalDecodable where Wrapped: Decodable {
  fileprivate static func decodeListElement(
    _ cv: ColumnValue,
    elementSchema: FieldSchema?
  ) throws -> Self {
    if cv == .null { return nil }
    if let result = decodePrimitive(cv, as: Wrapped.self) { return result }
    if let schema = elementSchema {
      if let result: Wrapped = try decodeSpecialType(cv, schema: schema) { return result }
      if case .struct(_, let ffiFields, _) = schema._ffi,
        case .struct(let childValues) = cv
      {
        let fields = ffiFields.map(FieldSchema.init)
        let childDecoder = DecoderImpl(values: childValues, schema: fields)
        return try Wrapped(from: childDecoder)
      }
    }
    throw DecodingError.typeMismatch(
      Wrapped.self,
      DecodingError.Context(
        codingPath: [],
        debugDescription: "Cannot decode Optional<\(Wrapped.self)> from \(cv)"
      )
    )
  }
}

// MARK: - Map Decoding Support

/// Private protocol that lets `Dictionary<K, V>` decode itself from a
/// `[ColumnValue]` list of `ColumnValue.struct([key, value])` items when the
/// column schema is a Parquet MAP.
private protocol _ParquetMapDecodable {
  static func _decodeMapEntries(
    _ entries: [ColumnValue],
    keySchema: FieldSchema,
    valueSchema: FieldSchema
  ) throws -> Self
}

extension Dictionary: _ParquetMapDecodable where Key: Decodable & Hashable, Value: Decodable {
  fileprivate static func _decodeMapEntries(
    _ entries: [ColumnValue],
    keySchema: FieldSchema,
    valueSchema: FieldSchema
  ) throws -> Self {
    var dict = Self()
    dict.reserveCapacity(entries.count)
    for entry in entries {
      guard case .struct(let fields) = entry, fields.count == 2 else { continue }
      guard
        let k: Key = (try decodeSpecialType(fields[0], schema: keySchema))
          ?? decodeColumnPrimitive(fields[0]),
        let v: Value = (try decodeSpecialType(fields[1], schema: valueSchema))
          ?? decodeColumnPrimitive(fields[1])
      else { continue }
      dict[k] = v
    }
    return dict
  }
}

/// Extracts a Swift primitive value from a `ColumnValue` without schema guidance.
/// Used for map key/value decoding where the types are always primitive.
private func decodeColumnPrimitive<T>(_ cv: ColumnValue) -> T? {
  switch cv {
  case .bool(let v): return v as? T
  case .int8(let v): return v as? T
  case .int16(let v): return v as? T
  case .int32(let v): return v as? T
  case .int64(let v): return v as? T
  case .uInt8(let v): return v as? T
  case .uInt16(let v): return v as? T
  case .uInt32(let v): return v as? T
  case .uInt64(let v): return v as? T
  case .float16(let v):
    if #available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *) {
      return Float16(v) as? T
    } else {
      return v as? T  // fallback: expose as Float
    }
  case .float32(let v): return v as? T
  case .float64(let v): return v as? T
  case .utf8(let v): return v as? T
  case .bytes(let v): return v as? T
  default: return nil
  }
}
