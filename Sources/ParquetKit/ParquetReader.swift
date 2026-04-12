// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import ParquetKitFFI

/// Reads ``ParquetCodable`` rows from a Parquet file as an `AsyncSequence`.
///
/// Iterate with `for try await` to read rows one at a time:
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetReaderUsage")
///
/// By default the file's schema is validated against `Row.parquetSchema` on
/// `init`; a mismatch throws ``ParquetError`` before any rows are read.
/// Pass a ``ParquetReaderConfiguration`` with `.lenient` compatibility to
/// allow reading files whose schema differs from the current `Row.parquetSchema`:
/// extra file columns are ignored and missing nullable columns decode as `nil`.
///
/// Thread-safe and `Sendable`. The `AsyncIterator` value type needs no additional
/// annotation.
public final class ParquetReader<Row: ParquetDecodable>: AsyncSequence, @unchecked Sendable {
  public typealias Element = Row

  private let handle: ReaderHandle
  private let decoder: ParquetDecoder

  /// Creates a reader for `url` and validates the file schema.
  ///
  /// - Parameters:
  ///   - url: File URL of the Parquet file to read.
  ///   - configuration: Reader configuration.  Defaults to ``ParquetReaderConfiguration/default``
  ///     which enforces an exact schema match.
  /// - Throws: ``ParquetError`` if the file cannot be opened or (in strict
  ///   mode) its schema doesn't match `Row.parquetSchema`.
  public init(url: URL, configuration: ParquetReaderConfiguration = .default) throws {
    let h = try mapParquetError { try ReaderHandle(path: url.path) }
    let ffiSchema = try mapParquetError { try h.schema() }
    let fileSchema = ffiSchema.map(FieldSchema.init)

    switch configuration.schemaCompatibility {
    case .strict:
      if fileSchema != Row.parquetSchema {
        throw ParquetError.schema(
          msg: "File schema does not match expected schema for \(Row.self)"
        )
      }
      self.handle = h
      self.decoder = ParquetDecoder(schema: Row.parquetSchema)

    case .lenient:
      // Project only the columns that are both in the file and in Row.parquetSchema.
      // Extra file columns are dropped by the projection; missing schema columns
      // remain absent from the values array so the decoder returns nil for
      // optional properties and throws keyNotFound for non-optional ones.
      let fileColumnNames = Set(fileSchema.map(\.fieldName))
      let projectedNames = Row.parquetSchema.map(\.fieldName).filter {
        fileColumnNames.contains($0)
      }
      let projectedHandle = try mapParquetError {
        try ReaderHandle.newProjected(path: url.path, columns: projectedNames)
      }
      let projectedSchema = Row.parquetSchema.filter { projectedNames.contains($0.fieldName) }
      self.handle = projectedHandle
      self.decoder = ParquetDecoder(schema: projectedSchema)
    }
  }

  public func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(handle: handle, decoder: decoder)
  }

  /// An iterator that decodes one row per `next()` call.
  public struct AsyncIterator: AsyncIteratorProtocol {
    let handle: ReaderHandle
    let decoder: ParquetDecoder

    /// Returns the next decoded row, or `nil` at end of file.
    ///
    /// - Throws: ``ParquetError`` or `DecodingError` if a row cannot be read or decoded.
    public mutating func next() async throws -> Row? {
      guard let values = try mapParquetError({ try handle.readRow() }) else { return nil }
      return try decoder.decode(Row.self, from: values)
    }
  }
}
