// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Foundation
import Testing

@testable import ParquetKit

// MARK: - Basic round-trip

@Test func simpleRoundTrip() async throws {
  let rows = [
    SimpleRow(id: 1, name: "Alice", score: 95.5),
    SimpleRow(id: 2, name: "Bob", score: 87.3),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func optionalFields() async throws {
  let rows = [
    OptionalRow(id: 1, name: "Alice"),
    OptionalRow(id: 2, name: nil),
    OptionalRow(id: 3, name: "Charlie"),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - All primitives with representative and boundary values

@Test func allPrimitives() async throws {
  let rows: [AllPrimitivesRow] = [
    // Representative values
    AllPrimitivesRow(
      boolVal: true, int8Val: -42, int16Val: 1000, int32Val: -100_000, int64Val: 9_000_000_000,
      uint8Val: 200, uint16Val: 60000, uint32Val: 4_000_000_000,
      uint64Val: 18_000_000_000_000_000_000,
      float32Val: 3.14, float64Val: 2.718281828, stringVal: "hello \u{1F600}"
    ),
    // Boundary: signed minima
    AllPrimitivesRow(
      boolVal: false, int8Val: Int8.min, int16Val: Int16.min, int32Val: Int32.min,
      int64Val: Int64.min,
      uint8Val: 0, uint16Val: 0, uint32Val: 0, uint64Val: 0,
      float32Val: -Float.greatestFiniteMagnitude, float64Val: -Double.greatestFiniteMagnitude,
      stringVal: ""
    ),
    // Boundary: signed/unsigned maxima
    AllPrimitivesRow(
      boolVal: true, int8Val: Int8.max, int16Val: Int16.max, int32Val: Int32.max,
      int64Val: Int64.max,
      uint8Val: UInt8.max, uint16Val: UInt16.max, uint32Val: UInt32.max, uint64Val: UInt64.max,
      float32Val: Float.greatestFiniteMagnitude, float64Val: Double.greatestFiniteMagnitude,
      stringVal: "unicode: \u{1F600}\u{0}"
    ),
    // Zero / false
    AllPrimitivesRow(
      boolVal: false, int8Val: 0, int16Val: 0, int32Val: 0, int64Val: 0,
      uint8Val: 0, uint16Val: 0, uint32Val: 0, uint64Val: 0,
      float32Val: 0.0, float64Val: 0.0, stringVal: "zero"
    ),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - encodeNil(forKey:)

@Test func encodeNilForKey() async throws {
  // ExplicitNilRow.encode(to:) calls container.encodeNil(forKey: .name) directly
  // when name is nil, exercising the KeyedEncodingContainer.encodeNil path.
  let rows = [
    ExplicitNilRow(id: 1, name: "Alice"),
    ExplicitNilRow(id: 2, name: nil),
    ExplicitNilRow(id: 3, name: "Charlie"),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Float special values

@Test func floatInfinity() async throws {
  // +∞ and −∞ are valid IEEE 754 values and must survive the round-trip.
  let rows = [
    Float32Row(value: Float.infinity),
    Float32Row(value: -Float.infinity),
  ]
  let result = try await writeAndRead(rows)
  #expect(result[0].value.isInfinite && result[0].value > 0)
  #expect(result[1].value.isInfinite && result[1].value < 0)
}

@Test func doubleInfinity() async throws {
  let rows = [
    Float64Row(value: Double.infinity),
    Float64Row(value: -Double.infinity),
  ]
  let result = try await writeAndRead(rows)
  #expect(result[0].value.isInfinite && result[0].value > 0)
  #expect(result[1].value.isInfinite && result[1].value < 0)
}

@Test func floatNaN() async throws {
  // NaN must survive the round-trip (NaN != NaN under IEEE 754, so use isNaN).
  let rows = [Float32Row(value: Float.nan)]
  let result = try await writeAndRead(rows)
  #expect(result[0].value.isNaN)
}

@Test func doubleNaN() async throws {
  let rows = [Float64Row(value: Double.nan)]
  let result = try await writeAndRead(rows)
  #expect(result[0].value.isNaN)
}

// MARK: - String edge cases

@Test func emptyString() async throws {
  let rows = [StringRow(value: ""), StringRow(value: "non-empty"), StringRow(value: "")]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Binary types

@Test func uuidRoundTrip() async throws {
  let rows = [
    UUIDRow(id: UUID(uuidString: "12345678-1234-5678-1234-567812345678")!),
    UUIDRow(id: UUID()),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func dataRoundTrip() async throws {
  let rows = [
    DataRow(blob: Data([0x00, 0xFF, 0xAB])),
    DataRow(blob: Data()),
    DataRow(blob: Data(repeating: 0x42, count: 100)),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

// MARK: - Decimal128

@Test func decimal128RoundTrip() async throws {
  let pos = Decimal128(high: 0, low: 12345)
  let neg = Decimal128(high: UInt64.max, low: UInt64.max)  // two's complement of -1
  let zero = Decimal128(high: 0, low: 0)
  let rows = [Decimal128Row(amount: pos), Decimal128Row(amount: neg), Decimal128Row(amount: zero)]
  let result = try await writeAndRead(rows)
  #expect(result[0].amount == pos)
  #expect(result[1].amount == neg)
  #expect(result[2].amount == zero)
}

@Test func decimal128ToBytesRoundTrip() {
  let d = Decimal128(high: 0x0102_0304_0506_0708, low: 0x090A_0B0C_0D0E_0F10)
  let bytes = d.toBytes()
  #expect(bytes.count == 16)
  #expect(bytes[0] == 0x01)
  #expect(bytes[8] == 0x09)
  #expect(Decimal128.fromBytes(bytes) == d)
}

@Test func decimal128Arithmetic() throws {
  let d = try Decimal128(from: Decimal(string: "123.45")!, scale: 2)
  #expect("\(d.toDecimal(scale: 2))" == "123.45")

  let neg = try Decimal128(from: Decimal(string: "-0.01")!, scale: 2)
  #expect(neg.high == UInt64.max)
  #expect(neg.low == UInt64.max)
  #expect("\(neg.toDecimal(scale: 2))" == "-0.01")

  let zero = try Decimal128(from: Decimal(0), scale: 5)
  #expect(zero.high == 0)
  #expect(zero.low == 0)
}

// MARK: - Float16

@available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *)
@Test func float16RoundTrip() async throws {
  let inputs: [Float16] = [0.0, 1.0, -1.0, 0.5, 100.0, -65504.0]
  let url = tempFileURL()
  defer { try? FileManager.default.removeItem(at: url) }
  let writer = try ParquetWriter<Float16Row>(url: url)
  for row in inputs.map({ Float16Row(score: $0) }) { try writer.write(row) }
  try writer.close()

  var read: [Float16Row] = []
  for try await r in try ParquetReader<Float16Row>(url: url) { read.append(r) }
  #expect(read.map(\.score) == inputs)
}
