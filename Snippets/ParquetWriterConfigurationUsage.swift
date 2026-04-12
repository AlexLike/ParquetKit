// Configure compression and row group size for bulk ingestion.
// snippet.hide
import Foundation
import ParquetKit

@Parquet
struct LogEntry: Codable {
  let requestId: Int64
  let path: String
  let statusCode: Int32
}

let outputURL = URL(fileURLWithPath: "/tmp/access-logs.parquet")
let entries: [LogEntry] = []

func runConfiguredWriter() throws {
  // snippet.show
  let config = ParquetWriterConfiguration(
    compression: .zstd(level: 5),
    rowGroupSize: 100_000
  )
  let writer = try ParquetWriter<LogEntry>(url: outputURL, configuration: config)
  for entry in entries {
    try writer.write(entry)
  }
  try writer.close()
  // snippet.hide
}
// snippet.show
