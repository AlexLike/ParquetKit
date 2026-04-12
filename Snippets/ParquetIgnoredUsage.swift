// snippet.hide
import ParquetKit

@Parquet
struct UserProfile: Codable {
  var accountId: Int64 = 0
  // snippet.show
  @ParquetIgnored var displayName: String = ""
  // snippet.hide
}
// snippet.show
