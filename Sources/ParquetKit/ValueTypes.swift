// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation

// MARK: - ParquetInterval

/// A Parquet INTERVAL value; three independent unsigned components stored as
/// `FIXED_LEN_BYTE_ARRAY(12)` in little-endian order.
///
/// This is distinct from `Duration`, which is a single nanosecond count.
/// Use ``init(months:days:milliseconds:)`` to construct values.
/// The ``Parquet()`` macro recognises `ParquetInterval`-typed properties automatically.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetIntervalUsage")
public struct ParquetInterval: Codable, Hashable, Sendable {
  /// Calendar months component.
  public var months: UInt32
  /// Calendar days component.
  public var days: UInt32
  /// Milliseconds component.
  public var milliseconds: UInt32

  /// Creates an interval from its three independent components.
  public init(months: UInt32, days: UInt32, milliseconds: UInt32) {
    self.months = months
    self.days = days
    self.milliseconds = milliseconds
  }

  /// Serialises to the 12-byte Parquet wire format: months, days, milliseconds
  /// each as a little-endian `UInt32`.
  public func toBytes() -> Data {
    var data = Data(count: 12)
    data.withUnsafeMutableBytes { buf in
      buf.storeBytes(of: months.littleEndian, toByteOffset: 0, as: UInt32.self)
      buf.storeBytes(of: days.littleEndian, toByteOffset: 4, as: UInt32.self)
      buf.storeBytes(of: milliseconds.littleEndian, toByteOffset: 8, as: UInt32.self)
    }
    return data
  }

  /// Reconstructs a `ParquetInterval` from its 12-byte Parquet wire representation.
  ///
  /// - Precondition: `data.count == 12`
  public static func fromBytes(_ data: Data) -> ParquetInterval {
    precondition(data.count == 12)
    return data.withUnsafeBytes { buf in
      let months = UInt32(littleEndian: buf.load(fromByteOffset: 0, as: UInt32.self))
      let days = UInt32(littleEndian: buf.load(fromByteOffset: 4, as: UInt32.self))
      let millis = UInt32(littleEndian: buf.load(fromByteOffset: 8, as: UInt32.self))
      return ParquetInterval(months: months, days: days, milliseconds: millis)
    }
  }
}

// MARK: - TimestampUnit

/// The time unit used when encoding a ``ParquetTimestamp`` column.
///
/// The unit is recorded in the column's ``FieldSchema`` and must match the
/// ``ParquetTimestamp/unit`` carried by each value.
public enum TimestampUnit: String, Codable, Sendable {
  /// Milliseconds since Unix epoch.
  case milliseconds
  /// Microseconds since Unix epoch.
  case microseconds
  /// Nanoseconds since Unix epoch.
  case nanoseconds
}

// MARK: - TimeUnit

/// The time unit used when encoding a ``ParquetTime`` column.
///
/// The unit determines the Parquet physical type:
/// - `.milliseconds` → `TIME(MILLIS)` (int32)
/// - `.microseconds` → `TIME(MICROS)` (int64)
/// - `.nanoseconds`  → `TIME(NANOS)`  (int64)
public enum TimeUnit: String, Codable, Sendable {
  /// Milliseconds since midnight (Parquet `TIME(MILLIS)`).
  case milliseconds
  /// Microseconds since midnight (Parquet `TIME(MICROS)`).
  case microseconds
  /// Nanoseconds since midnight (Parquet `TIME(NANOS)`).
  case nanoseconds
}

// MARK: - Decimal128

/// A 128-bit fixed-precision decimal value for Parquet storage.
///
/// The unscaled integer is stored as a big-endian two's-complement value split
/// across two `UInt64` fields. Precision and scale live in the column's
/// ``FieldSchema``, not here; supply them via ``ParquetDecimal(precision:scale:)``
/// on the enclosing property.
///
/// @Snippet(path: "ParquetKit/Snippets/Decimal128Usage")
public struct Decimal128: Codable, Hashable, Sendable {
  /// The high 64 bits of the unscaled integer (big-endian two's-complement).
  public var high: UInt64
  /// The low 64 bits of the unscaled integer (big-endian two's-complement).
  public var low: UInt64

  /// Creates a `Decimal128` from explicit high and low words.
  public init(high: UInt64, low: UInt64) {
    self.high = high
    self.low = low
  }

  /// Creates a `Decimal128` by scaling a `Decimal` value.
  ///
  /// Multiplies `decimal` by `10^scale` and stores the result as a 128-bit
  /// two's-complement integer. Precision is not validated here.
  ///
  /// @Snippet(path: "ParquetKit/Snippets/Decimal128ConstructionUsage")
  ///
  /// - Parameters:
  ///   - decimal: The base-10 value to scale.
  ///   - scale: The number of decimal places (non-negative).
  public init(from decimal: Decimal, scale: Int) throws {
    var scaled = decimal
    for _ in 0..<scale {
      scaled = scaled * 10
    }
    let int64Value = NSDecimalNumber(decimal: scaled).int64Value
    if int64Value >= 0 {
      self.high = 0
      self.low = UInt64(bitPattern: int64Value)
    } else {
      self.high = UInt64.max
      self.low = UInt64(bitPattern: int64Value)
    }
  }

  /// Converts the stored unscaled integer back to a `Decimal`, dividing by `10^scale`.
  public func toDecimal(scale: Int) -> Decimal {
    let isNegative = high >> 63 == 1
    var value: Decimal
    if isNegative {
      let notHigh = ~high
      let notLow = ~low
      let (addedLow, overflow) = notLow.addingReportingOverflow(1)
      let addedHigh = notHigh &+ (overflow ? 1 : 0)
      value =
        Decimal(addedHigh)
        * Decimal(sign: .minus, exponent: 0, significand: Decimal(UInt64(1) << 63) * 2)
        + Decimal(addedLow) * Decimal(-1)
    } else {
      value =
        Decimal(high) * Decimal(sign: .plus, exponent: 0, significand: Decimal(UInt64(1) << 63) * 2)
        + Decimal(low)
    }
    for _ in 0..<scale {
      value = value / 10
    }
    return value
  }

  /// The 16-byte big-endian representation used for Parquet column storage.
  public func toBytes() -> [UInt8] {
    var bytes = [UInt8](repeating: 0, count: 16)
    var h = high.bigEndian
    var l = low.bigEndian
    withUnsafeBytes(of: &h) { bytes.replaceSubrange(0..<8, with: $0) }
    withUnsafeBytes(of: &l) { bytes.replaceSubrange(8..<16, with: $0) }
    return bytes
  }

  /// Reconstructs a `Decimal128` from its 16-byte big-endian representation.
  ///
  /// - Precondition: `bytes.count == 16`
  public static func fromBytes(_ bytes: [UInt8]) -> Decimal128 {
    precondition(bytes.count == 16)
    let high = bytes.withUnsafeBufferPointer { ptr -> UInt64 in
      ptr.baseAddress!.withMemoryRebound(to: UInt64.self, capacity: 1) {
        UInt64(bigEndian: $0.pointee)
      }
    }
    let low = bytes.dropFirst(8).withUnsafeBufferPointer { ptr -> UInt64 in
      ptr.baseAddress!.withMemoryRebound(to: UInt64.self, capacity: 1) {
        UInt64(bigEndian: $0.pointee)
      }
    }
    return Decimal128(high: high, low: low)
  }
}

// MARK: - ParquetTimestamp

/// A timestamp value that carries its own time unit and UTC-adjustment flag for Parquet encoding.
///
/// The unit is recorded in both the value and the column ``FieldSchema``.
/// Use ``init(date:unit:)`` on the property to specify which unit
/// the ``FieldSchema`` should emit.
///
/// @Snippet(path: "ParquetKit/Snippets/ParquetTimestampUsage")
public struct ParquetTimestamp: Codable, Hashable, Sendable {
  /// The point in time.
  public var date: Date
  /// The time unit for encoding.
  public var unit: TimestampUnit
  /// Whether this timestamp is UTC-adjusted (`isAdjustedToUTC=true` in Parquet).
  ///
  /// When `true`, the column is written as `Timestamp(unit, Some("UTC"))`, which
  /// tools like PyArrow and DuckDB interpret as a UTC instant. When `false`
  /// (the default), the column has no timezone annotation and uses wall-clock semantics.
  public var isAdjustedToUTC: Bool

  /// Creates a timestamp for a given date and unit with wall-clock semantics.
  public init(date: Date, unit: TimestampUnit) {
    self.date = date
    self.unit = unit
    self.isAdjustedToUTC = false
  }

  /// Creates a timestamp with explicit UTC-adjustment flag.
  public init(date: Date, unit: TimestampUnit, isAdjustedToUTC: Bool) {
    self.date = date
    self.unit = unit
    self.isAdjustedToUTC = isAdjustedToUTC
  }

  /// Converts the timestamp to the raw `Int64` value stored in Parquet.
  public func toInt64() -> Int64 {
    let interval = date.timeIntervalSince1970
    switch unit {
    case .milliseconds: return Int64(interval * 1_000)
    case .microseconds: return Int64(interval * 1_000_000)
    case .nanoseconds: return Int64(interval * 1_000_000_000)
    }
  }

  /// Reconstructs a ``ParquetTimestamp`` from the raw `Int64` value and its unit.
  public static func fromInt64(_ value: Int64, unit: TimestampUnit) -> ParquetTimestamp {
    let interval: TimeInterval
    switch unit {
    case .milliseconds: interval = TimeInterval(value) / 1_000
    case .microseconds: interval = TimeInterval(value) / 1_000_000
    case .nanoseconds: interval = TimeInterval(value) / 1_000_000_000
    }
    return ParquetTimestamp(date: Date(timeIntervalSince1970: interval), unit: unit)
  }
}

// MARK: - ParquetDate

/// A calendar date stored as days since the Unix epoch (1970-01-01), matching
/// Parquet's `DATE` physical type.
public struct ParquetDate: Codable, Hashable, Sendable {
  /// Days since 1970-01-01.  Negative values represent dates before the epoch.
  public var daysSinceEpoch: Int32

  /// Creates a date from an explicit day offset.
  public init(daysSinceEpoch: Int32) {
    self.daysSinceEpoch = daysSinceEpoch
  }

  /// Creates a date by truncating a `Date` to whole days (UTC).
  public init(_ date: Date) {
    self.daysSinceEpoch = Int32(date.timeIntervalSince1970 / 86400)
  }

  /// The corresponding `Date` at midnight UTC on this calendar day.
  public var date: Date {
    Date(timeIntervalSince1970: TimeInterval(daysSinceEpoch) * 86400)
  }
}

// MARK: - ParquetTime

/// A time-of-day value for Parquet `TIME` columns.
///
/// The `unit` determines both the physical Parquet type and the resolution of
/// ``valueSinceMidnight``:
/// - `.milliseconds` → `TIME(MILLIS)`; `int32`, millis since midnight
/// - `.microseconds` → `TIME(MICROS)`; `int64`, micros since midnight
/// - `.nanoseconds`  → `TIME(NANOS)` ; `int64`, nanos  since midnight
///
/// Use ``ParquetTime(unit:)`` on the property to specify a non-default unit;
/// an undecorated `ParquetTime` property defaults to `.microseconds`.
public struct ParquetTime: Codable, Hashable, Sendable {
  /// Raw time-of-day value in the column's time unit.
  public var valueSinceMidnight: Int64
  /// The time unit for this value.
  public var unit: TimeUnit

  // MARK: Convenience initialisers

  /// Creates a microsecond-precision time value.
  ///
  /// Backward-compatible initialiser; existing call sites that pass
  /// `microsecondsSinceMidnight:` continue to compile unchanged.
  public init(microsecondsSinceMidnight: Int64) {
    self.valueSinceMidnight = microsecondsSinceMidnight
    self.unit = .microseconds
  }

  /// Creates a millisecond-precision time value.
  public init(millisecondsSinceMidnight: Int64) {
    self.valueSinceMidnight = millisecondsSinceMidnight
    self.unit = .milliseconds
  }

  /// Creates a nanosecond-precision time value.
  public init(nanosecondsSinceMidnight: Int64) {
    self.valueSinceMidnight = nanosecondsSinceMidnight
    self.unit = .nanoseconds
  }

  // MARK: Computed accessors

  /// Microseconds since midnight, converting from the stored unit as needed.
  public var microsecondsSinceMidnight: Int64 {
    switch unit {
    case .microseconds: return valueSinceMidnight
    case .milliseconds: return valueSinceMidnight * 1_000
    case .nanoseconds: return valueSinceMidnight / 1_000
    }
  }
}
