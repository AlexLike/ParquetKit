// Read files whose schema may differ from the current model.
// snippet.hide
import Foundation
import ParquetKit

// v2 of the model adds `score`; older files won't have that column.
@Parquet
struct UserRecord: Codable {
  let userId: Int64
  let email: String
  var score: Double?
}

let archiveURL = URL(fileURLWithPath: "/tmp/users-v1.parquet")

func runLenientReader() async throws {
  // snippet.show
  let config = ParquetReaderConfiguration(schemaCompatibility: .lenient)
  for try await record in try ParquetReader<UserRecord>(url: archiveURL, configuration: config) {
    // score is nil for rows from older files that lack the column
    print(record.email, record.score ?? 0)
  }
  // snippet.hide
}
// snippet.show
