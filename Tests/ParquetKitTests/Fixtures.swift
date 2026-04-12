// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import Testing

@testable import ParquetKit

// MARK: - Helpers

func tempFileURL() -> URL {
  FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".parquet")
}

func writeAndRead<Row: ParquetCodable>(_ rows: [Row], config: ParquetWriterConfiguration? = nil)
  async throws -> [Row]
{
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<Row>(url: url, configuration: config)
  for row in rows { try writer.write(row) }
  try writer.close()
  var result: [Row] = []
  for try await row in try ParquetReader<Row>(url: url) { result.append(row) }
  return result
}

// MARK: - Primitive fixtures

struct SimpleRow: ParquetCodable, Equatable {
  var id: Int64
  var name: String
  var score: Double

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .primitive(name: "name", type: .utf8, nullable: false),
    .primitive(name: "score", type: .float64, nullable: false),
  ]
}

struct OptionalRow: ParquetCodable, Equatable {
  var id: Int64
  var name: String?

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .primitive(name: "name", type: .utf8, nullable: true),
  ]
}

struct AllPrimitivesRow: ParquetCodable, Equatable {
  var boolVal: Bool
  var int8Val: Int8
  var int16Val: Int16
  var int32Val: Int32
  var int64Val: Int64
  var uint8Val: UInt8
  var uint16Val: UInt16
  var uint32Val: UInt32
  var uint64Val: UInt64
  var float32Val: Float
  var float64Val: Double
  var stringVal: String

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "boolVal", type: .bool, nullable: false),
    .primitive(name: "int8Val", type: .int8, nullable: false),
    .primitive(name: "int16Val", type: .int16, nullable: false),
    .primitive(name: "int32Val", type: .int32, nullable: false),
    .primitive(name: "int64Val", type: .int64, nullable: false),
    .primitive(name: "uint8Val", type: .uInt8, nullable: false),
    .primitive(name: "uint16Val", type: .uInt16, nullable: false),
    .primitive(name: "uint32Val", type: .uInt32, nullable: false),
    .primitive(name: "uint64Val", type: .uInt64, nullable: false),
    .primitive(name: "float32Val", type: .float32, nullable: false),
    .primitive(name: "float64Val", type: .float64, nullable: false),
    .primitive(name: "stringVal", type: .utf8, nullable: false),
  ]
}

struct StringRow: ParquetCodable, Equatable {
  var value: String
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "value", type: .utf8, nullable: false)
  ]
}

struct Float32Row: ParquetCodable {
  var value: Float
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "value", type: .float32, nullable: false)
  ]
}

struct Float64Row: ParquetCodable {
  var value: Double
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "value", type: .float64, nullable: false)
  ]
}

struct UUIDRow: ParquetCodable, Equatable {
  var id: UUID
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .fixedBytes(size: 16), nullable: false)
  ]
}

struct DataRow: ParquetCodable, Equatable {
  var blob: Data
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "blob", type: .bytes, nullable: false)
  ]
}

struct Decimal128Row: ParquetCodable, Equatable {
  var amount: Decimal128
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "amount", type: .decimal128(precision: 10, scale: 2), nullable: false)
  ]
}

@available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *)
struct Float16Row: ParquetCodable {
  var score: Float16
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "score", type: .float16, nullable: false)
  ]
}

// MARK: - Temporal fixtures

struct TimestampRow: ParquetCodable, Equatable {
  var createdAt: ParquetTimestamp
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "createdAt", type: .timestampUs, nullable: false)
  ]
}

struct TimestampMsRow: ParquetCodable {
  var ts: ParquetTimestamp
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "ts", type: .timestampMs, nullable: false)
  ]
}

struct TimestampNsRow: ParquetCodable {
  var ts: ParquetTimestamp
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "ts", type: .timestampNs, nullable: false)
  ]
}

struct TimestampUtcRow: ParquetCodable {
  var ts: ParquetTimestamp
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "ts", type: .timestampUsUtc, nullable: false)
  ]
}

struct DateRow: ParquetCodable, Equatable {
  var date: ParquetDate
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "date", type: .date32, nullable: false)
  ]
}

struct TimeRow: ParquetCodable, Equatable {
  var time: ParquetTime
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "time", type: .timeUs, nullable: false)
  ]
}

struct TimeMsRow: ParquetCodable, Equatable {
  var time: ParquetTime
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "time", type: .timeMs, nullable: false)
  ]
}

struct TimeNsRow: ParquetCodable, Equatable {
  var time: ParquetTime
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "time", type: .timeNs, nullable: false)
  ]
}

struct IntervalRow: ParquetCodable, Equatable {
  var period: ParquetInterval
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "period", type: .interval, nullable: false)
  ]
}

// MARK: - Collection fixtures

struct ListRow: ParquetCodable, Equatable {
  var tags: [String]
  static let parquetSchema: [FieldSchema] = [
    .list(
      name: "tags", element: .primitive(name: "item", type: .utf8, nullable: false), nullable: false
    )
  ]
}

struct NestedListRow: ParquetCodable, Equatable {
  var matrix: [[String]]
  static let parquetSchema: [FieldSchema] = [
    .list(
      name: "matrix",
      element: .list(
        name: "item", element: .primitive(name: "item", type: .utf8, nullable: false),
        nullable: false),
      nullable: false
    )
  ]
}

struct NestedListOfNullablesRow: ParquetCodable, Equatable {
  var batches: [[Int32?]]

  static let parquetSchema: [FieldSchema] = [
    .list(
      name: "batches",
      element: .list(
        name: "item", element: .primitive(name: "item", type: .int32, nullable: true),
        nullable: false),
      nullable: false
    )
  ]

  init(batches: [[Int32?]]) { self.batches = batches }
  enum CodingKeys: CodingKey { case batches }

  func encode(to encoder: Encoder) throws {
    var keyed = encoder.container(keyedBy: CodingKeys.self)
    var outer = keyed.nestedUnkeyedContainer(forKey: .batches)
    for batch in batches {
      var inner = outer.nestedUnkeyedContainer()
      for v in batch {
        if let v { try inner.encode(v) } else { try inner.encodeNil() }
      }
    }
  }

  init(from decoder: Decoder) throws {
    let keyed = try decoder.container(keyedBy: CodingKeys.self)
    var outer = try keyed.nestedUnkeyedContainer(forKey: .batches)
    var result: [[Int32?]] = []
    while !outer.isAtEnd {
      var inner = try outer.nestedUnkeyedContainer()
      var batch: [Int32?] = []
      while !inner.isAtEnd {
        if try inner.decodeNil() {
          batch.append(nil)
        } else {
          batch.append(try inner.decode(Int32.self))
        }
      }
      result.append(batch)
    }
    batches = result
  }
}

struct NullableListRow: ParquetCodable, Equatable {
  var values: [Int32?]

  init(values: [Int32?]) { self.values = values }
  enum CodingKeys: CodingKey { case values }

  func encode(to encoder: Encoder) throws {
    var keyed = encoder.container(keyedBy: CodingKeys.self)
    var unkeyed = keyed.nestedUnkeyedContainer(forKey: .values)
    for v in values {
      if let v { try unkeyed.encode(v) } else { try unkeyed.encodeNil() }
    }
  }

  init(from decoder: Decoder) throws {
    let keyed = try decoder.container(keyedBy: CodingKeys.self)
    var unkeyed = try keyed.nestedUnkeyedContainer(forKey: .values)
    var result: [Int32?] = []
    while !unkeyed.isAtEnd {
      if try unkeyed.decodeNil() {
        result.append(nil)
      } else {
        result.append(try unkeyed.decode(Int32.self))
      }
    }
    values = result
  }

  static let parquetSchema: [FieldSchema] = [
    .list(
      name: "values", element: .primitive(name: "item", type: .int32, nullable: true),
      nullable: false)
  ]
}

struct NestedInfo: ParquetCodable, Equatable {
  var key: String
  var value: Int32

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "key", type: .utf8, nullable: false),
    .primitive(name: "value", type: .int32, nullable: false),
  ]
}

struct NestedRow: ParquetCodable, Equatable {
  var id: Int64
  var info: NestedInfo

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .structType(name: "info", fields: NestedInfo.parquetSchema, nullable: false),
  ]
}

struct OptionalStructRow: ParquetCodable, Equatable {
  var id: Int64
  var info: NestedInfo?

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .structType(name: "info", fields: NestedInfo.parquetSchema, nullable: true),
  ]
}

struct ListOfStructsRow: ParquetCodable, Equatable {
  var items: [NestedInfo]

  static let parquetSchema: [FieldSchema] = [
    .list(
      name: "items",
      element: .structType(name: "item", fields: NestedInfo.parquetSchema, nullable: false),
      nullable: false
    )
  ]
}

struct MapRow: ParquetCodable {
  var labels: [String: Int64]

  static let parquetSchema: [FieldSchema] = [
    .map(
      name: "labels",
      keyType: .primitive(name: "key", type: .utf8, nullable: false),
      valueType: .primitive(name: "value", type: .int64, nullable: true),
      nullable: false
    )
  ]
}

// MARK: - CodingKeys fixture

struct CodingKeysRow: ParquetCodable, Equatable {
  var userId: Int64
  var userName: String

  enum CodingKeys: String, CodingKey {
    case userId = "user_id"
    case userName = "user_name"
  }

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "user_id", type: .int64, nullable: false),
    .primitive(name: "user_name", type: .utf8, nullable: false),
  ]
}

// MARK: - encodeNil fixture

/// Uses a hand-written encode(to:) that calls encodeNil(forKey:) directly.
/// This exercises the KeyedEncodingContainer.encodeNil path, which synthesised
/// Codable never calls (it uses encodeIfPresent instead).
struct ExplicitNilRow: ParquetCodable, Equatable {
  var id: Int64
  var name: String?

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .primitive(name: "name", type: .utf8, nullable: true),
  ]

  enum CodingKeys: CodingKey { case id, name }

  func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    try container.encode(id, forKey: .id)
    if let name {
      try container.encode(name, forKey: .name)
    } else {
      try container.encodeNil(forKey: .name)  // exercises encodeNil(forKey:)
    }
  }

  init(id: Int64, name: String?) {
    self.id = id
    self.name = name
  }
  init(from decoder: Decoder) throws {
    let c = try decoder.container(keyedBy: CodingKeys.self)
    id = try c.decode(Int64.self, forKey: .id)
    name = try c.decodeIfPresent(String.self, forKey: .name)
  }
}

// MARK: - Schema evolution fixtures

/// Reads only `id` and `name`; a strict subset of `SimpleRow` (which also has `score`).
struct PartialSimpleRow: ParquetCodable, Equatable {
  var id: Int64
  var name: String

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .primitive(name: "name", type: .utf8, nullable: false),
  ]
}

/// Reads `id`, `name`, and an optional column `extra` not present in `SimpleRow` files.
struct ExtendedOptionalRow: ParquetCodable, Equatable {
  var id: Int64
  var name: String
  var extra: String?

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .primitive(name: "name", type: .utf8, nullable: false),
    .primitive(name: "extra", type: .utf8, nullable: true),
  ]
}

/// Requires `id` plus a non-nullable column `required` not present in `SimpleRow` files.
struct RequiredMissingRow: ParquetCodable {
  var id: Int64
  var required: String

  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "id", type: .int64, nullable: false),
    .primitive(name: "required", type: .utf8, nullable: false),
  ]
}

// MARK: - Error fixtures

/// Schema-incompatible with SimpleRow: different column name.
struct IncompatibleRow: ParquetCodable {
  var x: Int32
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "x", type: .int32, nullable: false)
  ]
}

/// Type-incompatible with SimpleRow: `score` declared as utf8, but the encoder produces Float64;
/// forces a TypeMismatch in the Rust value layer when writing.
struct TypeMismatchRow: ParquetCodable {
  var score: Double
  static let parquetSchema: [FieldSchema] = [
    .primitive(name: "score", type: .utf8, nullable: false)
  ]
}
