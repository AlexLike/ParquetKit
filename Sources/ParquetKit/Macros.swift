// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

// MARK: - Macro declarations

/// Synthesizes ``ParquetEncodable``, ``ParquetDecodable``, or both conformances
/// for a struct, depending on whether it adopts `Encodable`, `Decodable`, or `Codable`.
///
/// Generates members on the type's extension:
/// - `static var parquetSchema: [FieldSchema]`; derived from stored properties
///   in declaration order, respecting ``ParquetIgnored()`` and `CodingKeys`.
/// - `static var defaultWriterConfiguration: ParquetWriterConfiguration`; only
///   emitted for `Encodable`/`Codable` types when at least one property carries
///   ``ParquetColumn(compression:encoding:enableDictionary:)``; otherwise the
///   protocol default (``ParquetWriterConfiguration/default``) is used.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetMacroUsage")
@attached(
  extension, conformances: ParquetEncodable, ParquetDecodable, names: named(parquetSchema),
  named(defaultWriterConfiguration))
public macro Parquet() = #externalMacro(module: "ParquetKitMacros", type: "ParquetMacro")

/// Specifies decimal precision and scale for a `Decimal128` property.
///
/// Required on every `Decimal128` property.  Omitting it is a compile error.
///
/// - Parameters:
///   - precision: Total number of significant decimal digits (1–38).
///   - scale: Number of digits after the decimal point (0 ≤ scale ≤ precision).
@attached(peer)
public macro ParquetDecimal(precision: UInt8, scale: UInt8) =
  #externalMacro(
    module: "ParquetKitMacros", type: "ParquetDecimalMacro")

/// Specifies the time unit for a `ParquetTimestamp` property.
///
/// Required on every `ParquetTimestamp` property.  Omitting it is a compile error.
///
/// - Parameters:
///   - unit: The timestamp precision; `.milliseconds`, `.microseconds`, or `.nanoseconds`.
///   - isAdjustedToUTC: When `true` the column is written as UTC-adjusted (`isAdjustedToUTC=true`
///     in Parquet), which tools such as PyArrow interpret as a UTC instant.  Defaults to `false`.
@attached(peer)
public macro ParquetTimestamp(unit: TimestampUnit, isAdjustedToUTC: Bool = false) =
  #externalMacro(
    module: "ParquetKitMacros", type: "ParquetTimestampMacro")

/// Specifies the time unit for a `ParquetTime` property.
///
/// Optional on `ParquetTime` properties. Without it, the column defaults to microsecond precision.
///
/// - Parameter unit: The time-of-day precision; `.milliseconds`, `.microseconds`, or `.nanoseconds`.
@attached(peer)
public macro ParquetTime(unit: TimeUnit) =
  #externalMacro(
    module: "ParquetKitMacros", type: "ParquetTimeMacro")

/// Marks a `Float`-typed property for Parquet encoding as half-precision (16-bit) float.
///
/// This annotation is only needed when the property type is `Float`, as a fallback for
/// OS versions before macOS 11 / iOS 14 where `Float16` is unavailable.
/// On macOS 11+, iOS 14+, tvOS 14+, watchOS 7+ use `Float16` directly; the
/// ``Parquet()`` macro recognises it automatically without any annotation.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetFloat16Usage")
@attached(peer)
public macro ParquetFloat16() =
  #externalMacro(module: "ParquetKitMacros", type: "ParquetFloat16Macro")

/// Excludes a property from the Parquet schema, encoder, and decoder.
///
/// The property must have a default value.  It is invisible to Parquet I/O;
/// its value is set to the default on decode and never written on encode.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetIgnoredUsage")
@attached(peer)
public macro ParquetIgnored() =
  #externalMacro(module: "ParquetKitMacros", type: "ParquetIgnoredMacro")

/// Specifies per-column writer configuration overrides for a property.
///
/// Affects only ``ParquetEncodable/defaultWriterConfiguration``; the schema is
/// unchanged. All parameters are optional; omitted ones inherit the file-level
/// default.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetColumnUsage")
///
/// - Parameters:
///   - compression: Per-column compression codec.
///   - encoding: Per-column encoding strategy.
///   - enableDictionary: Whether to build a dictionary page for this column.
@attached(peer)
public macro ParquetColumn(
  compression: ParquetWriterConfiguration.Compression? = nil,
  encoding: ParquetWriterConfiguration.Encoding? = nil,
  enableDictionary: Bool? = nil
) = #externalMacro(module: "ParquetKitMacros", type: "ParquetColumnMacro")
