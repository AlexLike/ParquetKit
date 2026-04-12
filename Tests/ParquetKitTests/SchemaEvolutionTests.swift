// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import Testing

@testable import ParquetKit

// MARK: - Strict mode (existing behavior)

@Test func strictModeRejectsExtraColumns() async throws {
  // Write SimpleRow (id/name/score), try to read as PartialSimpleRow (id/name) in strict mode.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<SimpleRow>(url: url)
  try writer.write(SimpleRow(id: 1, name: "Alice", score: 9.5))
  try writer.close()

  #expect {
    _ = try ParquetReader<PartialSimpleRow>(url: url)
  } throws: { error in
    guard case ParquetError.schema = error else { return false }
    return true
  }
}

@Test func strictModeRejectsMissingColumns() async throws {
  // Write PartialSimpleRow (id/name), try to read as SimpleRow (id/name/score) in strict mode.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<PartialSimpleRow>(url: url)
  try writer.write(PartialSimpleRow(id: 1, name: "Bob"))
  try writer.close()

  #expect {
    _ = try ParquetReader<SimpleRow>(url: url)
  } throws: { error in
    guard case ParquetError.schema = error else { return false }
    return true
  }
}

// MARK: - Lenient mode: extra file columns are ignored

@Test func lenientIgnoresExtraFileColumns() async throws {
  // Write SimpleRow (id/name/score), read as PartialSimpleRow (id/name); score is ignored.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<SimpleRow>(url: url)
  try writer.write(SimpleRow(id: 1, name: "Alice", score: 9.5))
  try writer.write(SimpleRow(id: 2, name: "Bob", score: 7.0))
  try writer.close()

  let config = ParquetReaderConfiguration(schemaCompatibility: .lenient)
  var rows: [PartialSimpleRow] = []
  for try await row in try ParquetReader<PartialSimpleRow>(url: url, configuration: config) {
    rows.append(row)
  }
  #expect(
    rows == [
      PartialSimpleRow(id: 1, name: "Alice"),
      PartialSimpleRow(id: 2, name: "Bob"),
    ])
}

// MARK: - Lenient mode: missing nullable columns decode as nil

@Test func lenientFillsMissingNullableWithNil() async throws {
  // Write PartialSimpleRow (id/name), read as ExtendedOptionalRow (id/name/extra?); extra is nil.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<PartialSimpleRow>(url: url)
  try writer.write(PartialSimpleRow(id: 10, name: "Carol"))
  try writer.close()

  let config = ParquetReaderConfiguration(schemaCompatibility: .lenient)
  var rows: [ExtendedOptionalRow] = []
  for try await row in try ParquetReader<ExtendedOptionalRow>(url: url, configuration: config) {
    rows.append(row)
  }
  #expect(rows == [ExtendedOptionalRow(id: 10, name: "Carol", extra: nil)])
}

// MARK: - Lenient mode: missing non-nullable column errors at row read time

@Test func lenientErrorsOnMissingNonNullableAtReadTime() async throws {
  // Write SimpleRow (id/name/score), read as RequiredMissingRow (id/required:String) in lenient mode.
  // The file has `id` but not `required`, which is non-nullable → DecodingError at row read.
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<SimpleRow>(url: url)
  try writer.write(SimpleRow(id: 1, name: "Dave", score: 5.0))
  try writer.close()

  let config = ParquetReaderConfiguration(schemaCompatibility: .lenient)
  // Init should succeed (no strict check), but iterating throws.
  let reader = try ParquetReader<RequiredMissingRow>(url: url, configuration: config)
  await #expect(throws: (any Error).self) {
    for try await _ in reader {}
  }
}

// MARK: - Lenient mode: identical schemas work like strict

@Test func lenientWithIdenticalSchemas() async throws {
  let rows = [
    SimpleRow(id: 1, name: "Alice", score: 1.0),
    SimpleRow(id: 2, name: "Bob", score: 2.0),
  ]
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<SimpleRow>(url: url)
  for row in rows { try writer.write(row) }
  try writer.close()

  let config = ParquetReaderConfiguration(schemaCompatibility: .lenient)
  var result: [SimpleRow] = []
  for try await row in try ParquetReader<SimpleRow>(url: url, configuration: config) {
    result.append(row)
  }
  #expect(result == rows)
}

// MARK: - Configuration defaults

@Test func readerConfigurationDefaults() {
  let config = ParquetReaderConfiguration()
  if case .strict = config.schemaCompatibility {
  } else {
    Issue.record("Expected .strict as default")
  }
  let defaultConfig = ParquetReaderConfiguration.default
  if case .strict = defaultConfig.schemaCompatibility {
  } else {
    Issue.record("Expected .strict as default for .default")
  }
}
