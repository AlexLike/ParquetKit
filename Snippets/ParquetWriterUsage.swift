// Write rows to a Parquet file.
// snippet.hide
import Foundation
import ParquetKit

@Parquet
struct SensorReading: Codable {
  let sensorId: Int64
  let location: String
}

let writerFileURL = URL(fileURLWithPath: "/tmp/readings.parquet")
let readings: [SensorReading] = []

func runParquetWriter() throws {
  // snippet.show
  let writer = try ParquetWriter<SensorReading>(url: writerFileURL)
  for reading in readings {
    try writer.write(reading)
  }
  try writer.close()
  // snippet.hide
}
// snippet.show
