// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import ParquetKitFFI

// MARK: - ParquetWriterConfiguration

/// Configuration for a ``ParquetWriter``.
///
/// The ``default`` configuration uses Snappy compression, 4096-row groups, and
/// dictionary encoding enabled globally.  Override individual settings or
/// add per-column overrides via ``columnOverrides``.
///
/// When a ``ParquetWriter`` is created, the caller-supplied configuration is
/// merged over the type's ``ParquetEncodable/defaultWriterConfiguration``:
/// file-level fields are replaced directly; ``columnOverrides`` is merged
/// key-by-key with the caller's entries winning.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetWriterConfigurationUsage")
public struct ParquetWriterConfiguration: Sendable {
  /// The default configuration: Snappy compression, 4096-row groups, dictionaries enabled.
  public static let `default` = ParquetWriterConfiguration()

  /// File-level compression codec applied to all columns that don't override it.
  public var compression: Compression = .snappy
  /// Maximum number of rows per row group.
  public var rowGroupSize: Int = 4096
  /// Target uncompressed size in bytes for each data page.
  public var dataPageSize: Int = 1_048_576
  /// Whether to build dictionary pages for all columns by default.
  public var enableDictionary: Bool = true
  /// Whether to write column statistics into row-group footers.
  public var enableStatistics: Bool = true
  /// Per-column overrides keyed by resolved schema field name.
  ///
  /// Keys must match the resolved column names used in the schema; raw
  /// `CodingKeys` string values if the type defines them, Swift property
  /// names otherwise.
  public var columnOverrides: [String: ColumnConfiguration] = [:]

  /// Creates a writer configuration with explicit values.
  public init(
    compression: Compression = .snappy,
    rowGroupSize: Int = 4096,
    dataPageSize: Int = 1_048_576,
    enableDictionary: Bool = true,
    enableStatistics: Bool = true,
    columnOverrides: [String: ColumnConfiguration] = [:]
  ) {
    self.compression = compression
    self.rowGroupSize = rowGroupSize
    self.dataPageSize = dataPageSize
    self.enableDictionary = enableDictionary
    self.enableStatistics = enableStatistics
    self.columnOverrides = columnOverrides
  }

  // MARK: - ColumnConfiguration

  /// Per-column encoding overrides that supplement the file-level defaults.
  ///
  /// Any `nil` field means "use the file-level default for this column."
  public struct ColumnConfiguration: Equatable, Sendable {
    /// Per-column compression override, or `nil` to inherit the file default.
    public var compression: Compression?
    /// Per-column encoding override, or `nil` to inherit the file default.
    public var encoding: Encoding?
    /// Per-column dictionary toggle, or `nil` to inherit the file default.
    public var enableDictionary: Bool?

    /// Creates a column configuration with optional per-field overrides.
    public init(
      compression: Compression? = nil,
      encoding: Encoding? = nil,
      enableDictionary: Bool? = nil
    ) {
      self.compression = compression
      self.encoding = encoding
      self.enableDictionary = enableDictionary
    }
  }

  // MARK: - Compression

  /// Compression codec for Parquet data pages.
  public enum Compression: Equatable, Sendable {
    /// No compression.
    case none
    /// Snappy compression (fast; good for general use).
    case snappy
    /// LZ4 compression.
    case lz4
    /// Gzip compression at the given level (1–9).
    case gzip(level: Int)
    /// Zstandard compression at the given level (1–22).
    case zstd(level: Int)
    /// Brotli compression at the given level (0–11).
    case brotli(level: Int)
  }

  // MARK: - Encoding

  /// Column encoding strategy for Parquet data pages.
  public enum Encoding: Equatable, Sendable {
    /// Plain encoding; values stored verbatim.
    case plain
    /// Delta binary packed encoding (good for sorted or slowly-changing integers and timestamps).
    case deltaBinaryPacked
    /// Delta length byte array (good for variable-length strings with similar lengths).
    case deltaLengthByteArray
    /// Delta byte array (good for strings with shared prefixes, e.g. URLs, paths).
    case deltaByteArray
    /// RLE dictionary (good for low-cardinality string or integer columns).
    case rleDictionary
  }

  // MARK: - Merging

  /// Returns a new configuration formed by overlaying `other` on top of `self`.
  ///
  /// File-level fields are taken from `other`.
  /// ``columnOverrides`` are merged key-by-key: entries from `other` override
  /// matching entries from `self`, and non-overlapping keys from both are kept.
  public func merged(with other: ParquetWriterConfiguration) -> ParquetWriterConfiguration {
    var result = other
    var merged = self.columnOverrides
    for (key, value) in other.columnOverrides {
      merged[key] = value
    }
    result.columnOverrides = merged
    return result
  }

  /// Converts this configuration to the FFI `WriterConfig` type.
  func toFFI() -> WriterConfig {
    let columnConfigs = columnOverrides.map { name, config in
      ColumnConfig(
        columnName: name,
        compression: config.compression.map { $0.toFFI() },
        encoding: config.encoding.map { $0.toFFI() },
        enableDictionary: config.enableDictionary
      )
    }
    return WriterConfig(
      compression: compression.toFFI(),
      rowGroupSize: UInt32(rowGroupSize),
      dataPageSize: UInt32(dataPageSize),
      enableDictionary: enableDictionary,
      enableStatistics: enableStatistics,
      columnConfigs: columnConfigs
    )
  }
}

// MARK: - FFI Conversions

extension ParquetWriterConfiguration.Compression {
  func toFFI() -> ParquetKitFFI.Compression {
    switch self {
    case .none: return .none
    case .snappy: return .snappy
    case .lz4: return .lz4
    case .gzip(let level): return .gzip(level: UInt8(level))
    case .zstd(let level): return .zstd(level: UInt8(level))
    case .brotli(let level): return .brotli(level: UInt8(level))
    }
  }
}

extension ParquetWriterConfiguration.Encoding {
  func toFFI() -> ParquetKitFFI.Encoding {
    switch self {
    case .plain: return .plain
    case .deltaBinaryPacked: return .deltaBinaryPacked
    case .deltaLengthByteArray: return .deltaLengthByteArray
    case .deltaByteArray: return .deltaByteArray
    case .rleDictionary: return .rleDictionary
    }
  }
}
