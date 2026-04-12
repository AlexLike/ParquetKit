// snippet.hide
import ParquetKit

@Parquet
struct Address: Codable {
  let street: String
  let city: String
}

struct Customer: ParquetCodable {
  let customerId: Int64
  let address: Address

  // snippet.show
  static var parquetSchema: [FieldSchema] {
    [
      .primitive(name: "customerId", type: .int64, nullable: false),
      .structType(
        name: "address",
        fields: Address.parquetSchema,
        nullable: false),
    ]
  }
  // snippet.hide
}
// snippet.show
