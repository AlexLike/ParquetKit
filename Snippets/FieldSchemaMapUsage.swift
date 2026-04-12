// snippet.hide
import ParquetKit

// snippet.show

struct Metrics: ParquetCodable {
  var labels: [String: Int64]

  static var parquetSchema: [FieldSchema] {
    [
      .map(
        name: "labels",
        keyType: .primitive(name: "key", type: .utf8, nullable: false),
        valueType: .primitive(name: "value", type: .int64, nullable: true),
        nullable: false)
    ]
  }
}
