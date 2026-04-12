// snippet.hide
import ParquetKit

@Parquet
struct AccessLog: Codable {
  // snippet.show
  @ParquetColumn(encoding: .deltaBinaryPacked)
  var requestId: Int64

  @ParquetColumn(compression: .zstd(level: 3), enableDictionary: true)
  var country: String
  // snippet.hide
}
// snippet.show
