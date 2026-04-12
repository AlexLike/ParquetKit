// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

/// A type that can be written to a Parquet file.
///
/// Adopt this protocol to use a type with ``ParquetWriter``.
/// ``parquetSchema`` describes the column layout;
/// ``defaultWriterConfiguration`` provides default encoding settings.
///
/// The ``Parquet()`` macro synthesizes both requirements from your struct's
/// stored properties when the type conforms to `Encodable` or `Codable`.
///
/// > Note: All `ParquetEncodable` types must also be `Sendable` because
/// > ``ParquetWriter`` is `Sendable`.
public protocol ParquetEncodable: Encodable, Sendable {
  /// The Parquet column schema for this type, in declaration order.
  ///
  /// Schema field names must match the coding keys used by this type.
  /// The ``Parquet()`` macro generates this automatically.
  static var parquetSchema: [FieldSchema] { get }

  /// The default writer configuration for this type.
  ///
  /// ``ParquetWriter`` merges the caller-supplied configuration over this value.
  /// The ``Parquet()`` macro generates a non-default implementation only when
  /// the struct has ``ParquetColumn(compression:encoding:enableDictionary:)``
  /// annotations.
  static var defaultWriterConfiguration: ParquetWriterConfiguration { get }
}

extension ParquetEncodable {
  /// Returns ``ParquetWriterConfiguration/default`` with no column overrides.
  public static var defaultWriterConfiguration: ParquetWriterConfiguration {
    .default
  }
}

/// A type that can be read from a Parquet file.
///
/// Adopt this protocol to use a type with ``ParquetReader``.
/// ``parquetSchema`` describes the expected column layout.
///
/// The ``Parquet()`` macro synthesizes this requirement from your struct's
/// stored properties when the type conforms to `Decodable` or `Codable`.
///
/// > Note: All `ParquetDecodable` types must also be `Sendable` because
/// > ``ParquetReader`` is `Sendable`.
public protocol ParquetDecodable: Decodable, Sendable {
  /// The Parquet column schema for this type, in declaration order.
  ///
  /// Schema field names must match the coding keys used by this type.
  /// The ``Parquet()`` macro generates this automatically.
  static var parquetSchema: [FieldSchema] { get }
}

/// A type that can be both written to and read from Parquet files.
///
/// Convenience composition of ``ParquetEncodable`` and ``ParquetDecodable``.
/// Use the individual protocols when only one direction is needed.
public typealias ParquetCodable = ParquetEncodable & ParquetDecodable
