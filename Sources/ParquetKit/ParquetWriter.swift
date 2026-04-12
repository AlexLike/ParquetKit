// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import ParquetKitFFI

/// Writes ``ParquetCodable`` rows to a Parquet file.
///
/// `ParquetWriter` is generic over the row type `Row`.  The schema and default
/// writer configuration are taken from `Row.parquetSchema` and
/// `Row.defaultWriterConfiguration`; the caller-supplied `configuration` is
/// merged on top (see ``ParquetWriterConfiguration/merged(with:)``).
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetWriterUsage")
///
/// Thread-safe and `Sendable`, but individual ``write(_:)`` calls are not
/// atomic with respect to each other.
public final class ParquetWriter<Row: ParquetEncodable>: @unchecked Sendable {
  private let handle: WriterHandle
  private let encoder: ParquetEncoder
  private let closeLock = NSLock()
  private var closed = false

  /// Creates a writer that appends rows to `url`.
  ///
  /// - Parameters:
  ///   - url: File URL to write to.  The file is created or truncated.
  ///   - configuration: Optional overrides merged over `Row.defaultWriterConfiguration`.
  ///     Caller entries take precedence key-by-key for column overrides.
  /// - Throws: ``ParquetError`` if the file cannot be opened or the configuration
  ///   contains unknown column names.
  public init(url: URL, configuration: ParquetWriterConfiguration? = nil) throws {
    let mergedConfig: ParquetWriterConfiguration
    if let configuration {
      mergedConfig = Row.defaultWriterConfiguration.merged(with: configuration)
    } else {
      mergedConfig = Row.defaultWriterConfiguration
    }

    let fieldNames = Set(Row.parquetSchema.map(\.fieldName))
    for key in mergedConfig.columnOverrides.keys {
      if !fieldNames.contains(key) {
        throw ParquetError.schema(msg: "Unknown column name in configuration: '\(key)'")
      }
    }

    self.handle = try mapParquetError {
      try WriterHandle(
        path: url.path,
        schema: Row.parquetSchema.ffiValues,
        config: mergedConfig.toFFI()
      )
    }
    self.encoder = ParquetEncoder(schema: Row.parquetSchema)
  }

  /// Encodes and appends a single row.
  ///
  /// Rows flush to disk automatically when the row group fills up
  /// (see ``ParquetWriterConfiguration/rowGroupSize``).
  /// Call ``close()`` to flush any remaining buffered rows.
  ///
  /// - Throws: ``ParquetError`` or `EncodingError` if encoding fails.
  public func write(_ row: Row) throws {
    let values = try encoder.encode(row)
    try mapParquetError { try handle.appendRow(values: values) }
  }

  /// Flushes remaining buffered rows and closes the file.
  ///
  /// Safe to call multiple times; subsequent calls are no-ops.
  /// Prefer calling this explicitly if you need to handle flush errors.
  /// If not called, `deinit` will attempt a best-effort flush on your behalf.
  ///
  /// - Throws: ``ParquetError`` if flushing or closing fails.
  public func close() throws {
    closeLock.lock()
    guard !closed else {
      closeLock.unlock()
      return
    }
    closed = true
    closeLock.unlock()
    try mapParquetError { try handle.close() }
  }

  deinit {
    guard !closed else { return }
    try? handle.close()
  }
}
