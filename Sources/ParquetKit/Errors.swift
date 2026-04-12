// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import ParquetKitFFI

// MARK: - ParquetError

/// An error thrown by Parquet I/O operations.
public enum ParquetError: Error, Sendable {
  /// A file I/O error; the file could not be opened, read, or written.
  case io(msg: String)
  /// A schema error; e.g. an unknown column name in a writer configuration,
  /// or a mismatch between the file schema and the expected schema on read.
  case schema(msg: String)
  /// A type mismatch between a column value and its schema type.
  case typeMismatch(msg: String)
  /// The file is not a valid Parquet file.
  case invalidFile(msg: String)
}

// MARK: Internal FFI conversion

extension ParquetError {
  init(_ ffi: ParquetKitFFI.ParquetError) {
    switch ffi {
    case .Io(let msg): self = .io(msg: msg)
    case .Schema(let msg): self = .schema(msg: msg)
    case .TypeMismatch(let msg): self = .typeMismatch(msg: msg)
    case .InvalidFile(let msg): self = .invalidFile(msg: msg)
    }
  }
}

// MARK: Helpers for wrapping FFI calls

/// Executes `block`, mapping any `ParquetKitFFI.ParquetError` to `ParquetError`.
func mapParquetError<T>(_ block: () throws -> T) throws -> T {
  do {
    return try block()
  } catch let e as ParquetKitFFI.ParquetError {
    throw ParquetError(e)
  }
}
