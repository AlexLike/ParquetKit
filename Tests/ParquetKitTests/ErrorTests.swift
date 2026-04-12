// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import Testing

@testable import ParquetKit

// MARK: - Schema errors

@Test func schemaMismatchOnRead() async throws {
  // Write SimpleRow (id/name/score), read as IncompatibleRow (x); column name mismatch.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<SimpleRow>(url: url)
  try writer.write(SimpleRow(id: 1, name: "x", score: 0))
  try writer.close()

  #expect {
    _ = try ParquetReader<IncompatibleRow>(url: url)
  } throws: { error in
    guard case ParquetError.schema = error else { return false }
    return true
  }
}

@Test func unknownColumnInConfig() async throws {
  // Column name in config that doesn't exist in the schema.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let config = ParquetWriterConfiguration(
    columnOverrides: ["nonexistent_column": .init(compression: .snappy)]
  )
  #expect {
    _ = try ParquetWriter<SimpleRow>(url: url, configuration: config)
  } throws: { error in
    guard case ParquetError.schema = error else { return false }
    return true
  }
}

// MARK: - I/O errors

@Test func readFromNonexistentFile() {
  let url = URL(fileURLWithPath: "/tmp/does_not_exist_\(UUID().uuidString).parquet")
  #expect {
    _ = try ParquetReader<SimpleRow>(url: url)
  } throws: { error in
    guard case ParquetError.io = error else { return false }
    return true
  }
}

// MARK: - Invalid file

@Test func invalidFileError() throws {
  // Write garbage bytes that are not a valid Parquet file.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  try Data([0x00, 0x01, 0xFF, 0xFE, 0xAB, 0xCD]).write(to: url)

  #expect {
    _ = try ParquetReader<SimpleRow>(url: url)
  } throws: { error in
    switch error {
    case ParquetError.invalidFile: return true
    case ParquetError.io: return true  // some backends surface this as Io
    default: return false
    }
  }
}

// MARK: - Type mismatch

@Test func typeMismatchOnWrite() async throws {
  // TypeMismatchRow.parquetSchema declares `score` as utf8, but the Swift encoder
  // produces ColumnValue::Float64 for a Double field; the Rust value layer rejects it.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<TypeMismatchRow>(url: url)
  #expect(throws: ParquetError.self) {
    try writer.write(TypeMismatchRow(score: 3.14))
  }
}

// MARK: - Boundary conditions

@Test func emptyFile() async throws {
  // Writing 0 rows then reading yields no rows (not an error).
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<SimpleRow>(url: url)
  try writer.close()
  var rows: [SimpleRow] = []
  for try await row in try ParquetReader<SimpleRow>(url: url) { rows.append(row) }
  #expect(rows.isEmpty)
}
