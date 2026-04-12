// snippet.hide
import ParquetKit

struct Post: ParquetCodable {
  let id: Int64
  let name: String?
  let tags: [String]

  // snippet.show
  static var parquetSchema: [FieldSchema] {
    [
      .primitive(name: "id", type: .int64, nullable: false),
      .primitive(name: "name", type: .utf8, nullable: true),
      .list(
        name: "tags",
        element: .primitive(name: "item", type: .utf8, nullable: false),
        nullable: false),
    ]
  }
  // snippet.hide
}
// snippet.show
