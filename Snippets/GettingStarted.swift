// snippet.hide
import Foundation
import ParquetKit

let fileURL = URL(fileURLWithPath: "/tmp/readings.parquet")
// snippet.show

@Parquet
struct SensorReading: Codable {
  var stationCode: String
  var isActive: Bool
  var temperatureCelsius: Double
  var errorCode: Int32?

  @ParquetTimestamp(unit: .microseconds, isAdjustedToUTC: true)
  var recordedAt: ParquetTimestamp

  @ParquetColumn(compression: .zstd(level: 3), enableDictionary: true)
  var region: String
}

// Write
let writer = try ParquetWriter<SensorReading>(url: fileURL)
try writer.write(
  SensorReading(
    stationCode: "NUUK-01",
    isActive: true,
    temperatureCelsius: -12.5,
    errorCode: nil,
    recordedAt: .init(date: Date(), unit: .microseconds, isAdjustedToUTC: true),
    region: "Greenland"
  ))
try writer.close()

// Read
for try await reading in try ParquetReader<SensorReading>(url: fileURL) {
  print(reading.stationCode, reading.temperatureCelsius)
}
