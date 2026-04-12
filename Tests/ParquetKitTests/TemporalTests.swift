// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import Testing

@testable import ParquetKit

// MARK: - Timestamp

@Test func timestampMicroseconds() async throws {
  let t = Date(timeIntervalSince1970: 1_700_000_000.123456)
  let result = try await writeAndRead([
    TimestampRow(createdAt: ParquetTimestamp(date: t, unit: .microseconds))
  ])
  #expect(result.count == 1)
  #expect(result[0].createdAt.unit == .microseconds)
  #expect(abs(result[0].createdAt.date.timeIntervalSince1970 - t.timeIntervalSince1970) < 1e-5)
}

@Test func timestampMilliseconds() async throws {
  let t = Date(timeIntervalSince1970: 1_700_000_000.5)
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<TimestampMsRow>(url: url)
  try writer.write(TimestampMsRow(ts: ParquetTimestamp(date: t, unit: .milliseconds)))
  try writer.close()
  var read: [TimestampMsRow] = []
  for try await r in try ParquetReader<TimestampMsRow>(url: url) { read.append(r) }
  #expect(read.count == 1)
  #expect(abs(read[0].ts.date.timeIntervalSince1970 - t.timeIntervalSince1970) < 1e-3)
}

@Test func timestampNanoseconds() async throws {
  let t = Date(timeIntervalSince1970: 1_700_000_000.000_001)
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<TimestampNsRow>(url: url)
  try writer.write(TimestampNsRow(ts: ParquetTimestamp(date: t, unit: .nanoseconds)))
  try writer.close()
  var read: [TimestampNsRow] = []
  for try await r in try ParquetReader<TimestampNsRow>(url: url) { read.append(r) }
  #expect(read.count == 1)
  #expect(abs(read[0].ts.date.timeIntervalSince1970 - t.timeIntervalSince1970) < 1e-5)
}

@Test func timestampUtcRoundTrip() async throws {
  let t = Date(timeIntervalSince1970: 1_700_000_000.0)
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<TimestampUtcRow>(url: url)
  try writer.write(
    TimestampUtcRow(ts: ParquetTimestamp(date: t, unit: .microseconds, isAdjustedToUTC: true)))
  try writer.close()
  var read: [TimestampUtcRow] = []
  for try await r in try ParquetReader<TimestampUtcRow>(url: url) { read.append(r) }
  #expect(read.count == 1)
  #expect(read[0].ts.isAdjustedToUTC == true)
  #expect(read[0].ts.unit == .microseconds)
  #expect(abs(read[0].ts.date.timeIntervalSince1970 - t.timeIntervalSince1970) < 1e-5)
}

@Test func timestampEpoch() async throws {
  // Unix epoch (t = 0) and a pre-epoch timestamp
  let epoch = Date(timeIntervalSince1970: 0)
  let preEpoch = Date(timeIntervalSince1970: -86_400)  // 1969-12-31
  let result = try await writeAndRead([
    TimestampRow(createdAt: ParquetTimestamp(date: epoch, unit: .microseconds)),
    TimestampRow(createdAt: ParquetTimestamp(date: preEpoch, unit: .microseconds)),
  ])
  #expect(result.count == 2)
  #expect(abs(result[0].createdAt.date.timeIntervalSince1970) < 1e-5)
  #expect(abs(result[1].createdAt.date.timeIntervalSince1970 - (-86_400)) < 1e-5)
}

// MARK: - Date

@Test func dateRoundTrip() async throws {
  let rows = [
    DateRow(date: ParquetDate(daysSinceEpoch: 19000)),
    DateRow(date: ParquetDate(daysSinceEpoch: -1)),
    DateRow(date: ParquetDate(daysSinceEpoch: 0)),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func dateFromDate() async throws {
  // 2024-03-15 00:00:00 UTC = 19797 days since 1970-01-01
  var cal = Calendar(identifier: .gregorian)
  cal.timeZone = TimeZone(identifier: "UTC")!
  var comps = DateComponents()
  comps.year = 2024
  comps.month = 3
  comps.day = 15
  let date = cal.date(from: comps)!
  #expect(ParquetDate(date).daysSinceEpoch == 19797)
}

// MARK: - Time

@Test func timeRoundTrip() async throws {
  let rows = [
    TimeRow(time: ParquetTime(microsecondsSinceMidnight: 0)),  // midnight
    TimeRow(time: ParquetTime(microsecondsSinceMidnight: 43_200_000_000)),  // noon
    TimeRow(time: ParquetTime(microsecondsSinceMidnight: 86_399_999_999)),  // 23:59:59.999999
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func timeMsRoundTrip() async throws {
  let rows = [
    TimeMsRow(time: ParquetTime(millisecondsSinceMidnight: 0)),  // midnight
    TimeMsRow(time: ParquetTime(millisecondsSinceMidnight: 43_200_000)),  // noon
    TimeMsRow(time: ParquetTime(millisecondsSinceMidnight: 86_399_999)),  // 23:59:59.999
  ]
  let result = try await writeAndRead(rows)
  #expect(result.count == 3)
  for (expected, actual) in zip(rows, result) {
    #expect(actual.time.unit == .milliseconds)
    #expect(actual.time.valueSinceMidnight == expected.time.valueSinceMidnight)
  }
}

@Test func timeNsRoundTrip() async throws {
  let rows = [
    TimeNsRow(time: ParquetTime(nanosecondsSinceMidnight: 0)),
    TimeNsRow(time: ParquetTime(nanosecondsSinceMidnight: 43_200_000_000_000)),  // noon
    TimeNsRow(time: ParquetTime(nanosecondsSinceMidnight: 86_399_999_999_999)),  // 23:59:59.999...
  ]
  let result = try await writeAndRead(rows)
  #expect(result.count == 3)
  for (expected, actual) in zip(rows, result) {
    #expect(actual.time.unit == .nanoseconds)
    #expect(actual.time.valueSinceMidnight == expected.time.valueSinceMidnight)
  }
}

@Test func timeUnitConversion() {
  let ms = ParquetTime(millisecondsSinceMidnight: 3_600_000)
  #expect(ms.microsecondsSinceMidnight == 3_600_000_000)

  let ns = ParquetTime(nanosecondsSinceMidnight: 3_600_000_000_000)
  #expect(ns.microsecondsSinceMidnight == 3_600_000_000)

  let us = ParquetTime(microsecondsSinceMidnight: 3_600_000_000)
  #expect(us.microsecondsSinceMidnight == 3_600_000_000)
}

// MARK: - Interval

@Test func intervalRoundTrip() async throws {
  let rows = [
    IntervalRow(period: ParquetInterval(months: 0, days: 0, milliseconds: 0)),
    IntervalRow(period: ParquetInterval(months: 12, days: 0, milliseconds: 0)),
    IntervalRow(period: ParquetInterval(months: 0, days: 15, milliseconds: 3_600_000)),
    IntervalRow(period: ParquetInterval(months: 2, days: 30, milliseconds: 500)),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func intervalToBytesRoundTrip() {
  let interval = ParquetInterval(months: 3, days: 14, milliseconds: 86_400_000)
  let bytes = interval.toBytes()
  #expect(bytes.count == 12)
  #expect(ParquetInterval.fromBytes(bytes) == interval)
}
