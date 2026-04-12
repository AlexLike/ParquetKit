// snippet.hide
import ParquetKit

// snippet.show

@Parquet
struct PageView: Codable {
  let sessionId: Int64
  let url: String
  @ParquetTimestamp(unit: .microseconds) var viewedAt: ParquetTimestamp
  @ParquetIgnored var cachedTitle: String = ""
}
