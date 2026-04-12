// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

#if canImport(ParquetKitMacros)
  import SwiftSyntaxMacros
  import SwiftSyntaxMacrosTestSupport
  import Testing

  @testable import ParquetKitMacros

  let testMacros: [String: Macro.Type] = [
    "Parquet": ParquetMacro.self,
    "ParquetDecimal": ParquetDecimalMacro.self,
    "ParquetTimestamp": ParquetTimestampMacro.self,
    "ParquetTime": ParquetTimeMacro.self,
    "ParquetFloat16": ParquetFloat16Macro.self,
    "ParquetIgnored": ParquetIgnoredMacro.self,
    "ParquetColumn": ParquetColumnMacro.self,
  ]

  // MARK: - Schema generation: primitive types

  @Test func flatStruct() {
    assertMacroExpansion(
      """
      @Parquet
      struct User: Codable {
        let id: Int64
        let name: String
        let score: Double
      }
      """,
      expandedSource: """
        struct User: Codable {
          let id: Int64
          let name: String
          let score: Double
        }

        extension User: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false),
              .primitive(name: "name", type: .utf8, nullable: false),
              .primitive(name: "score", type: .float64, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func allPrimitiveTypes() {
    // Verifies every entry in the macro's primitiveMap
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let a: Bool
        let b: Int8
        let c: Int16
        let d: Int32
        let e: Int64
        let f: UInt8
        let g: UInt16
        let h: UInt32
        let i: UInt64
        let j: Float
        let k: Double
        let l: String
        let m: Data
        let n: UUID
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let a: Bool
          let b: Int8
          let c: Int16
          let d: Int32
          let e: Int64
          let f: UInt8
          let g: UInt16
          let h: UInt32
          let i: UInt64
          let j: Float
          let k: Double
          let l: String
          let m: Data
          let n: UUID
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "a", type: .bool, nullable: false),
              .primitive(name: "b", type: .int8, nullable: false),
              .primitive(name: "c", type: .int16, nullable: false),
              .primitive(name: "d", type: .int32, nullable: false),
              .primitive(name: "e", type: .int64, nullable: false),
              .primitive(name: "f", type: .uInt8, nullable: false),
              .primitive(name: "g", type: .uInt16, nullable: false),
              .primitive(name: "h", type: .uInt32, nullable: false),
              .primitive(name: "i", type: .uInt64, nullable: false),
              .primitive(name: "j", type: .float32, nullable: false),
              .primitive(name: "k", type: .float64, nullable: false),
              .primitive(name: "l", type: .utf8, nullable: false),
              .primitive(name: "m", type: .bytes, nullable: false),
              .primitive(name: "n", type: .fixedBytes(size: 16), nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func optionalFields() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let id: Int64
        let name: String?
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let id: Int64
          let name: String?
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false),
              .primitive(name: "name", type: .utf8, nullable: true)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  // MARK: - Schema generation: annotated types

  @Test func parquetDecimalAnnotation() {
    // @ParquetDecimal correctly emits .decimal128(precision:scale:)
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetDecimal(precision: 10, scale: 2) var amount: Decimal128
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var amount: Decimal128
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "amount", type: .decimal128(precision: 10, scale: 2), nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetTimestampMilliseconds() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetTimestamp(unit: .milliseconds) var ts: ParquetTimestamp
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var ts: ParquetTimestamp
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "ts", type: .timestampMs, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetTimestampMicroseconds() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetTimestamp(unit: .microseconds) var ts: ParquetTimestamp
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var ts: ParquetTimestamp
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "ts", type: .timestampUs, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetTimestampNanoseconds() {
    // Verifies the "nanoseconds" branch; the default fallback is .timestampUs so a typo would be silent
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetTimestamp(unit: .nanoseconds) var ts: ParquetTimestamp
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var ts: ParquetTimestamp
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "ts", type: .timestampNs, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetDuration() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        var elapsed: Duration
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var elapsed: Duration
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "elapsed", type: .durationNs, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  // MARK: - Schema generation: collections and nesting

  @Test func arrayTypeGeneratesList() {
    // Exercises the hasPrefix("[") branch in resolveSchemaEntry
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let tags: [String]
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let tags: [String]
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .list(name: "tags", element: .primitive(name: "item", type: .utf8, nullable: false), nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func arrayOfOptionalsGeneratesNullableElement() {
    // [String?] → list with nullable element (element type stripped of ?, nullable: true)
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let tags: [String?]
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let tags: [String?]
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .list(name: "tags", element: .primitive(name: "item", type: .utf8, nullable: true), nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func nestedArrayGeneratesNestedList() {
    // [[String]] → .list(element: .list(element: .primitive))
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let matrix: [[String]]
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let matrix: [[String]]
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .list(name: "matrix", element: .list(name: "item", element: .primitive(name: "item", type: .utf8, nullable: false), nullable: false), nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func nestedArrayOfOptionalsGeneratesNullableInnerElement() {
    // [[String?]] → outer list, inner list with nullable elements
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let batches: [[String?]]
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let batches: [[String?]]
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .list(name: "batches", element: .list(name: "item", element: .primitive(name: "item", type: .utf8, nullable: true), nullable: false), nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func listOfStructsGeneratesStructElement() {
    // Exercises the array branch when the element type is not a known primitive;
    // previously emitted an error; now falls through to structType.
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let items: [LineItem]
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let items: [LineItem]
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .list(name: "items", element: .structType(name: "item", fields: LineItem.parquetSchema, nullable: false), nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func nestedStructGeneratesStructType() {
    // Exercises the fallthrough to .structType for an unrecognised type name
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let id: Int64
        let info: NestedInfo
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let id: Int64
          let info: NestedInfo
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false),
              .structType(name: "info", fields: NestedInfo.parquetSchema, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  // MARK: - Writer configuration synthesis

  @Test func parquetColumnGeneratesConfig() {
    // @ParquetColumn on any property emits defaultWriterConfiguration
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let id: Int64
        @ParquetColumn(encoding: .deltaBinaryPacked) let sessionId: Int64
        @ParquetColumn(compression: .zstd(level: 3), enableDictionary: true) let country: String
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let id: Int64
          let sessionId: Int64
          let country: String
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false),
              .primitive(name: "sessionId", type: .int64, nullable: false),
              .primitive(name: "country", type: .utf8, nullable: false)
            ]
          }

          static var defaultWriterConfiguration: ParquetWriterConfiguration {
            var config = ParquetWriterConfiguration.default
            config.columnOverrides = [
              "sessionId": .init(encoding: .deltaBinaryPacked),
              "country": .init(compression: .zstd(level: 3), enableDictionary: true),
            ]
            return config
          }
        }
        """,
      macros: testMacros
    )
  }

  // MARK: - CodingKeys

  @Test func codingKeysRemap() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let userId: Int64
        let userName: String

        enum CodingKeys: String, CodingKey {
          case userId = "user_id"
          case userName = "user_name"
        }
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let userId: Int64
          let userName: String

          enum CodingKeys: String, CodingKey {
            case userId = "user_id"
            case userName = "user_name"
          }
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "user_id", type: .int64, nullable: false),
              .primitive(name: "user_name", type: .utf8, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  // MARK: - @ParquetIgnored

  @Test func ignoredProperty() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let id: Int64
        @ParquetIgnored var cached: String = ""
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let id: Int64
          var cached: String = ""
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  // MARK: - Diagnostics (errors)

  @Test func ignoredWithoutDefault() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let id: Int64
        @ParquetIgnored var cached: String
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let id: Int64
          var cached: String
        }
        """,
      diagnostics: [
        DiagnosticSpec(
          message: "@ParquetIgnored property must have a default value", line: 4, column: 3)
      ],
      macros: testMacros
    )
  }

  @Test func decimal128WithoutAnnotation() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        var amount: Decimal128
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var amount: Decimal128
        }
        """,
      diagnostics: [
        DiagnosticSpec(
          message: "Decimal128 requires @ParquetDecimal(precision:scale:)", line: 3, column: 3)
      ],
      macros: testMacros
    )
  }

  @Test func bareDecimalType() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        var amount: Decimal
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var amount: Decimal
        }
        """,
      diagnostics: [
        DiagnosticSpec(
          message: "Use Decimal128 with @ParquetDecimal(precision:scale:)", line: 3, column: 3)
      ],
      macros: testMacros
    )
  }

  @Test func parquetColumnOnStructColumn() {
    // @ParquetColumn on a struct-typed column must emit an error
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        let id: Int64
        @ParquetColumn(encoding: .plain) let info: SomeNestedType
      }
      """,
      expandedSource: """
        struct Row: Codable {
          let id: Int64
          let info: SomeNestedType
        }
        """,
      diagnostics: [
        DiagnosticSpec(
          message:
            "@ParquetColumn cannot be applied to a struct column; annotate the struct's own properties instead",
          line: 4, column: 3)
      ],
      macros: testMacros
    )
  }

  // MARK: - New type annotations

  @Test func parquetTimeMilliseconds() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetTime(unit: .milliseconds)
        var wakeAt: ParquetTime
      }
      """,
      expandedSource: """
        struct Row: Codable {
          @ParquetTime(unit: .milliseconds)
          var wakeAt: ParquetTime
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "wakeAt", type: .timeMs, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetTimeNanoseconds() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetTime(unit: .nanoseconds)
        var wakeAt: ParquetTime
      }
      """,
      expandedSource: """
        struct Row: Codable {
          @ParquetTime(unit: .nanoseconds)
          var wakeAt: ParquetTime
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "wakeAt", type: .timeNs, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetTimeDefaultsToMicroseconds() {
    // Undecorated ParquetTime still defaults to .timeUs for backward compat
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        var wakeAt: ParquetTime
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var wakeAt: ParquetTime
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "wakeAt", type: .timeUs, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetTimestampUtc() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetTimestamp(unit: .microseconds, isAdjustedToUTC: true)
        var eventTime: ParquetTimestamp
      }
      """,
      expandedSource: """
        struct Row: Codable {
          @ParquetTimestamp(unit: .microseconds, isAdjustedToUTC: true)
          var eventTime: ParquetTimestamp
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "eventTime", type: .timestampUsUtc, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func float16AutomaticMapping() {
    // Float16 is recognized automatically; no annotation needed.
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        var embedding: Float16
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var embedding: Float16
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "embedding", type: .float16, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func parquetFloat16FallbackOnFloat() {
    // @ParquetFloat16 on Float is the fallback for older OS without Float16.
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        @ParquetFloat16
        var embedding: Float
      }
      """,
      expandedSource: """
        struct Row: Codable {
          @ParquetFloat16
          var embedding: Float
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "embedding", type: .float16, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func dictionaryFieldIsRejected() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        var labels: [String: Int64]
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var labels: [String: Int64]
        }
        """,
      diagnostics: [
        DiagnosticSpec(
          message:
            "Dictionary fields are not supported by the @Parquet macro. Implement parquetSchema manually and use FieldSchema.map(name:keyType:valueType:nullable:).",
          line: 3, column: 3)
      ],
      macros: testMacros
    )
  }

  // MARK: - Encodable / Decodable only

  @Test func encodableOnly() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Encodable {
        let id: Int64
        let name: String
      }
      """,
      expandedSource: """
        struct Row: Encodable {
          let id: Int64
          let name: String
        }

        extension Row: ParquetEncodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false),
              .primitive(name: "name", type: .utf8, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func decodableOnly() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Decodable {
        let id: Int64
        let name: String
      }
      """,
      expandedSource: """
        struct Row: Decodable {
          let id: Int64
          let name: String
        }

        extension Row: ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false),
              .primitive(name: "name", type: .utf8, nullable: false)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func decodableOnlyIgnoresParquetColumn() {
    // @ParquetColumn on a Decodable-only type emits a warning and no config.
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Decodable {
        @ParquetColumn(encoding: .deltaBinaryPacked) let id: Int64
      }
      """,
      expandedSource: """
        struct Row: Decodable {
          @ParquetColumn(encoding: .deltaBinaryPacked) let id: Int64
        }

        extension Row: ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "id", type: .int64, nullable: false)
            ]
          }
        }
        """,
      diagnostics: [
        DiagnosticSpec(
          message: "@ParquetColumn has no effect on a Decodable-only type",
          line: 3, column: 3,
          severity: .warning)
      ],
      macros: testMacros
    )
  }

  @Test func parquetIntervalAutomatic() {
    // ParquetInterval no longer requires an annotation; the type name is sufficient.
    assertMacroExpansion(
      """
      @Parquet
      struct Row: Codable {
        var term: ParquetInterval
        var optTerm: ParquetInterval?
      }
      """,
      expandedSource: """
        struct Row: Codable {
          var term: ParquetInterval
          var optTerm: ParquetInterval?
        }

        extension Row: ParquetEncodable, ParquetDecodable {
          static var parquetSchema: [FieldSchema] {
            [
              .primitive(name: "term", type: .interval, nullable: false),
              .primitive(name: "optTerm", type: .interval, nullable: true)
            ]
          }
        }
        """,
      macros: testMacros
    )
  }

  @Test func noCodableConformanceIsAnError() {
    assertMacroExpansion(
      """
      @Parquet
      struct Row {
        let id: Int64
      }
      """,
      expandedSource: """
        struct Row {
          let id: Int64
        }
        """,
      diagnostics: [
        DiagnosticSpec(
          message:
            "@Parquet requires the type to conform to Encodable, Decodable, or Codable",
          line: 1, column: 1)
      ],
      macros: testMacros
    )
  }
#endif
