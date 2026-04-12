// snippet.hide
import ParquetKit

// snippet.show

@Parquet
struct Order: Codable {
  @ParquetDecimal(precision: 10, scale: 2)
  var amount: Decimal128
}
