// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import Testing

@testable import ParquetKit

// MARK: - Lists

@Test func listColumn() async throws {
  let rows = [ListRow(tags: ["swift", "parquet"]), ListRow(tags: []), ListRow(tags: ["rust"])]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func listWithNullElements() async throws {
  let rows = [
    NullableListRow(values: [1, nil, 3]),
    NullableListRow(values: [nil]),
    NullableListRow(values: []),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func listOfStructs() async throws {
  let rows = [
    ListOfStructsRow(items: [NestedInfo(key: "a", value: 1), NestedInfo(key: "b", value: 2)]),
    ListOfStructsRow(items: []),
    ListOfStructsRow(items: [NestedInfo(key: "c", value: 3)]),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Nested lists

@Test func nestedListOfStrings() async throws {
  let rows = [
    NestedListRow(matrix: [["a", "b"], ["c"], []]),
    NestedListRow(matrix: [[]]),
    NestedListRow(matrix: []),
    NestedListRow(matrix: [["x"], ["y", "z"]]),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func nestedListOfNullableInts() async throws {
  let rows = [
    NestedListOfNullablesRow(batches: [[1, nil, 3], [nil, 5]]),
    NestedListOfNullablesRow(batches: [[]]),
    NestedListOfNullablesRow(batches: []),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Structs

@Test func nestedStruct() async throws {
  let rows = [
    NestedRow(id: 1, info: NestedInfo(key: "count", value: 42)),
    NestedRow(id: 2, info: NestedInfo(key: "total", value: -7)),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func optionalStructColumn() async throws {
  let rows = [
    OptionalStructRow(id: 1, info: NestedInfo(key: "x", value: 10)),
    OptionalStructRow(id: 2, info: nil),
    OptionalStructRow(id: 3, info: NestedInfo(key: "y", value: -5)),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Map

@Test func mapRoundTrip() async throws {
  let rows: [MapRow] = [
    MapRow(labels: ["alpha": 1, "beta": 2, "gamma": 3]),
    MapRow(labels: [:]),
    MapRow(labels: ["x": 42]),
  ]
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<MapRow>(url: url)
  for row in rows { try writer.write(row) }
  try writer.close()
  var read: [MapRow] = []
  for try await r in try ParquetReader<MapRow>(url: url) { read.append(r) }
  #expect(read.count == rows.count)
  for (expected, actual) in zip(rows, read) {
    #expect(actual.labels == expected.labels)
  }
}
