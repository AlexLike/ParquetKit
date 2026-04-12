// Read rows from a Parquet file.
// snippet.hide
import Foundation
import ParquetKit

@Parquet
struct SensorReading: Codable {
  let sensorId: Int64
  let location: String
}

func process(_ reading: SensorReading) {}

let readerFileURL = URL(fileURLWithPath: "/tmp/readings.parquet")

func runParquetReader() async throws {
  // snippet.show
  let reader = try ParquetReader<SensorReading>(url: readerFileURL)
  for try await reading in reader {
    process(reading)
  }
  // snippet.hide
}
// snippet.show
