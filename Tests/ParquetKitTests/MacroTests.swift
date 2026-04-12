// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import Testing

@testable import ParquetKit

// MARK: - @Parquet decorated types

@Parquet
struct MacroEvent: Codable, Equatable {
  var id: Int64
  var name: String
  var score: Double?
  var tags: [String]
}

@Parquet
struct MacroUIntRow: Codable, Equatable {
  var a: UInt8
  var b: UInt16
  var c: UInt32
  var d: UInt64
}

// A realistic order-processing schema that exercises the full macro feature surface:
//   • nested @Parquet structs (struct columns)
//   • optional nested struct (nullable struct column)
//   • list of @Parquet structs ([MacroLineItem])
//   • list of primitives with nullable elements ([String?])
//   • optional primitive (String? couponCode)
//   • @ParquetIgnored cached field
//   • @ParquetColumn encoding override
//   • CodingKeys remapping on multiple fields

@Parquet
struct MacroAddress: Codable, Equatable {
  var street: String
  var city: String
  var countryCode: String
}

@Parquet
struct MacroLineItem: Codable, Equatable {
  var sku: String
  var quantity: Int32
  var priceCents: Int64
}

@Parquet
struct MacroOrder: Codable, Equatable {
  var orderId: Int64
  var customerEmail: String
  var shippingAddress: MacroAddress
  var billingAddress: MacroAddress?
  var items: [MacroLineItem]
  var tags: [String?]
  var couponCode: String?
  var totalCents: Int64
  @ParquetIgnored var cachedDisplay: String = ""
  @ParquetColumn(encoding: .deltaBinaryPacked)
  var createdMs: Int64

  enum CodingKeys: String, CodingKey {
    case orderId = "order_id"
    case customerEmail = "customer_email"
    case shippingAddress = "shipping_address"
    case billingAddress = "billing_address"
    case items
    case tags
    case couponCode = "coupon_code"
    case totalCents = "total_cents"
    case createdMs = "created_ms"
    // cachedDisplay intentionally omitted; @ParquetIgnored with default value
  }
}

// MARK: - Tests

@Test func parquetMacroEndToEnd() async throws {
  #expect(MacroEvent.parquetSchema.count == 4)
  #expect(MacroEvent.parquetSchema[0].fieldName == "id")
  #expect(MacroEvent.parquetSchema[0].primitiveType == .int64)
  #expect(MacroEvent.parquetSchema[0].isNullable == false)
  #expect(MacroEvent.parquetSchema[1].fieldName == "name")
  #expect(MacroEvent.parquetSchema[2].fieldName == "score")
  #expect(MacroEvent.parquetSchema[2].isNullable == true)
  #expect(MacroEvent.parquetSchema[3].fieldName == "tags")
  #expect(MacroEvent.parquetSchema[3].listElement != nil)

  let rows = [
    MacroEvent(id: 1, name: "launch", score: 9.5, tags: ["swift", "parquet"]),
    MacroEvent(id: 2, name: "crash", score: nil, tags: []),
  ]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func parquetMacroWithUIntFields() async throws {
  #expect(MacroUIntRow.parquetSchema[0].primitiveType == .uInt8)
  #expect(MacroUIntRow.parquetSchema[1].primitiveType == .uInt16)
  #expect(MacroUIntRow.parquetSchema[2].primitiveType == .uInt32)
  #expect(MacroUIntRow.parquetSchema[3].primitiveType == .uInt64)

  let rows = [MacroUIntRow(a: 255, b: 65535, c: .max, d: .max)]
  #expect(try await writeAndRead(rows) == rows)
}

@Test func parquetMacroComplexNestedSchema() async throws {
  // ── Schema shape ─────────────────────────────────────────────────────
  let schema = MacroOrder.parquetSchema
  #expect(schema.count == 9)  // cachedDisplay is @ParquetIgnored

  #expect(schema[0].fieldName == "order_id")
  #expect(schema[0].primitiveType == .int64)
  #expect(schema[0].isNullable == false)

  #expect(schema[1].fieldName == "customer_email")
  #expect(schema[1].primitiveType == .utf8)

  #expect(schema[2].fieldName == "shipping_address")
  #expect(schema[2].isNullable == false)
  #expect(schema[2].structFields?.count == 3)

  #expect(schema[3].fieldName == "billing_address")
  #expect(schema[3].isNullable == true)
  #expect(schema[3].structFields?.count == 3)

  #expect(schema[4].fieldName == "items")
  #expect(schema[4].listElement?.structFields != nil)

  #expect(schema[5].fieldName == "tags")
  #expect(schema[5].listElement?.primitiveType == .utf8)
  #expect(schema[5].listElement?.isNullable == true)

  #expect(schema[6].fieldName == "coupon_code")
  #expect(schema[6].isNullable == true)
  #expect(schema[6].primitiveType == .utf8)

  #expect(schema[7].fieldName == "total_cents")
  #expect(schema[8].fieldName == "created_ms")

  #expect(MacroOrder.defaultWriterConfiguration.columnOverrides["created_ms"] != nil)
  #expect(
    MacroOrder.defaultWriterConfiguration.columnOverrides["created_ms"]?.encoding
      == .deltaBinaryPacked
  )

  // ── Data ─────────────────────────────────────────────────────────────
  let home = MacroAddress(street: "1 Infinite Loop", city: "Cupertino", countryCode: "US")
  let office = MacroAddress(street: "1 Hacker Way", city: "Menlo Park", countryCode: "US")
  let widget = MacroLineItem(sku: "WGT-001", quantity: 2, priceCents: 999)
  let gadget = MacroLineItem(sku: "GDG-042", quantity: 1, priceCents: 4999)

  let rows: [MacroOrder] = [
    MacroOrder(
      orderId: 1001, customerEmail: "alice@example.com",
      shippingAddress: home, billingAddress: office,
      items: [widget, gadget], tags: ["priority", nil, "gift"],
      couponCode: "SAVE10", totalCents: 7_997,
      cachedDisplay: "ignored", createdMs: 1_700_000_000_000
    ),
    MacroOrder(
      orderId: 1002, customerEmail: "bob@example.com",
      shippingAddress: home, billingAddress: nil,
      items: [], tags: [],
      couponCode: nil, totalCents: 0,
      cachedDisplay: "also ignored", createdMs: 1_700_000_001_000
    ),
    MacroOrder(
      orderId: 1003, customerEmail: "carol@example.com",
      shippingAddress: office, billingAddress: home,
      items: [widget], tags: [nil, "parquet", nil, "uniffi", nil],
      couponCode: "VERYLONGCOUPONCODE2024", totalCents: 999,
      cachedDisplay: "ignored too", createdMs: 1_700_000_002_000
    ),
  ]

  let result = try await writeAndRead(rows)
  #expect(result.count == 3)
  for (expected, actual) in zip(rows, result) {
    #expect(actual.orderId == expected.orderId)
    #expect(actual.customerEmail == expected.customerEmail)
    #expect(actual.shippingAddress == expected.shippingAddress)
    #expect(actual.billingAddress == expected.billingAddress)
    #expect(actual.items == expected.items)
    #expect(actual.tags == expected.tags)
    #expect(actual.couponCode == expected.couponCode)
    #expect(actual.totalCents == expected.totalCents)
    #expect(actual.createdMs == expected.createdMs)
    #expect(actual.cachedDisplay == "")  // @ParquetIgnored: default value, not written value
  }
}
