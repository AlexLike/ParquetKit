// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

// MARK: - ParquetReaderConfiguration

/// Configuration for a ``ParquetReader``.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetReaderConfigurationUsage")
public struct ParquetReaderConfiguration: Sendable {

  // MARK: - SchemaCompatibility

  /// Controls how the file schema is matched against `Row.parquetSchema`.
  public enum SchemaCompatibility: Sendable {
    /// The file schema must exactly match `Row.parquetSchema` (default).
    ///
    /// A mismatch throws ``ParquetError/schema(msg:)`` before any rows are read.
    case strict

    /// Extra file columns are silently ignored; missing columns fill with `nil`
    /// for nullable properties and raise a `DecodingError.keyNotFound` at row
    /// read time for non-nullable ones.
    ///
    /// Use this when reading files produced by an older or newer schema version.
    case lenient
  }

  /// How the file schema is matched against the expected row schema.
  public var schemaCompatibility: SchemaCompatibility

  /// The default configuration: strict schema matching.
  public static let `default` = ParquetReaderConfiguration()

  /// Creates a reader configuration.
  ///
  /// - Parameter schemaCompatibility: Schema matching strategy.  Defaults to `.strict`.
  public init(schemaCompatibility: SchemaCompatibility = .strict) {
    self.schemaCompatibility = schemaCompatibility
  }
}
