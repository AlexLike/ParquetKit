// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Testing

@testable import ParquetKit

// MARK: - CodingKeys

@Test func codingKeysRemap() async throws {
  let rows = [
    CodingKeysRow(userId: 1, userName: "Alice"), CodingKeysRow(userId: 2, userName: "Bob"),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Row groups

@Test func multipleRowGroups() async throws {
  let config = ParquetWriterConfiguration(rowGroupSize: 3)
  let rows = (0..<7).map { SimpleRow(id: Int64($0), name: "row\($0)", score: Double($0) * 1.5) }
  #expect(try await writeAndRead(rows, config: config) == rows)
}

@Test func singleRow() async throws {
  let rows = [SimpleRow(id: 42, name: "solo", score: 1.0)]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Configuration merging

@Test func configurationMerging() throws {
  let base = ParquetWriterConfiguration(
    compression: .snappy,
    columnOverrides: ["a": .init(compression: .zstd(level: 3))]
  )
  let merger = ParquetWriterConfiguration(
    compression: .none,
    columnOverrides: ["b": .init(encoding: .plain)]
  )
  let merged = base.merged(with: merger)

  #expect(merged.compression == .none)
  #expect(merged.columnOverrides.count == 2)
  #expect(merged.columnOverrides["a"] != nil)
  #expect(merged.columnOverrides["b"] != nil)
}

@Test func callerConfigOverridesColumnDefault() async throws {
  let config = ParquetWriterConfiguration(
    compression: .zstd(level: 3),
    columnOverrides: ["id": .init(encoding: .deltaBinaryPacked)]
  )
  let rows = [SimpleRow(id: 1, name: "a", score: 1.0), SimpleRow(id: 2, name: "b", score: 2.0)]
  #expect(try await writeAndRead(rows, config: config) == rows)
}

// MARK: - Compression codecs

@Test func compressionGzip() async throws {
  let rows = [SimpleRow(id: 1, name: "gzip", score: 3.14)]
  #expect(
    try await writeAndRead(rows, config: ParquetWriterConfiguration(compression: .gzip(level: 6)))
      == rows)
}

@Test func compressionLz4() async throws {
  let rows = [SimpleRow(id: 1, name: "lz4", score: 2.71)]
  #expect(
    try await writeAndRead(rows, config: ParquetWriterConfiguration(compression: .lz4)) == rows)
}

@Test func compressionBrotli() async throws {
  let rows = [SimpleRow(id: 1, name: "brotli", score: 1.41)]
  #expect(
    try await writeAndRead(rows, config: ParquetWriterConfiguration(compression: .brotli(level: 4)))
      == rows)
}

// MARK: - Schema property accessors

@Test func fieldSchemaProperties() {
  let primitive = FieldSchema.primitive(name: "score", type: .float64, nullable: true)
  #expect(primitive.fieldName == "score")
  #expect(primitive.isNullable == true)
  #expect(primitive.primitiveType == .float64)
  #expect(primitive.listElement == nil)
  #expect(primitive.structFields == nil)
  #expect(primitive.mapKeyType == nil)
  #expect(primitive.mapValueType == nil)

  let element = FieldSchema.primitive(name: "item", type: .utf8, nullable: false)
  let list = FieldSchema.list(name: "tags", element: element, nullable: false)
  #expect(list.fieldName == "tags")
  #expect(list.isNullable == false)
  #expect(list.listElement == element)
  #expect(list.primitiveType == nil)
  #expect(list.structFields == nil)

  let structType = FieldSchema.structType(name: "info", fields: [primitive], nullable: false)
  #expect(structType.fieldName == "info")
  #expect(structType.structFields == [primitive])
  #expect(structType.listElement == nil)

  let keySchema = FieldSchema.primitive(name: "key", type: .utf8, nullable: false)
  let valueSchema = FieldSchema.primitive(name: "value", type: .int64, nullable: true)
  let map = FieldSchema.map(
    name: "labels", keyType: keySchema, valueType: valueSchema, nullable: false)
  #expect(map.fieldName == "labels")
  #expect(map.mapKeyType == keySchema)
  #expect(map.mapValueType == valueSchema)
  #expect(map.primitiveType == nil)
  #expect(map.listElement == nil)
  #expect(map.structFields == nil)
}

@Test func primitiveTypeEquality() {
  #expect(PrimitiveType.int64 == PrimitiveType.int64)
  #expect(PrimitiveType.fixedBytes(size: 16) == PrimitiveType.fixedBytes(size: 16))
  #expect(PrimitiveType.fixedBytes(size: 16) != PrimitiveType.fixedBytes(size: 32))
  #expect(
    PrimitiveType.decimal128(precision: 10, scale: 2)
      == PrimitiveType.decimal128(precision: 10, scale: 2))
  #expect(
    PrimitiveType.decimal128(precision: 10, scale: 2)
      != PrimitiveType.decimal128(precision: 10, scale: 3))
}

@Test func configurationEquality() {
  #expect(ParquetWriterConfiguration.Compression.snappy == .snappy)
  #expect(ParquetWriterConfiguration.Compression.zstd(level: 3) == .zstd(level: 3))
  #expect(ParquetWriterConfiguration.Compression.zstd(level: 3) != .zstd(level: 5))
  #expect(ParquetWriterConfiguration.Compression.gzip(level: 6) != .snappy)
  #expect(ParquetWriterConfiguration.Encoding.deltaBinaryPacked == .deltaBinaryPacked)
  #expect(ParquetWriterConfiguration.Encoding.plain != .rleDictionary)

  let col1 = ParquetWriterConfiguration.ColumnConfiguration(compression: .snappy, encoding: .plain)
  let col2 = ParquetWriterConfiguration.ColumnConfiguration(compression: .snappy, encoding: .plain)
  let col3 = ParquetWriterConfiguration.ColumnConfiguration(compression: .lz4, encoding: .plain)
  #expect(col1 == col2)
  #expect(col1 != col3)
}
