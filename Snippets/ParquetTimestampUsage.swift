// snippet.hide
import ParquetKit

@Parquet
struct FlightRecord: Codable {
  // snippet.show
  @ParquetTimestamp(unit: .microseconds)
  var scheduledDeparture: ParquetTimestamp  // local wall-clock time

  @ParquetTimestamp(unit: .microseconds, isAdjustedToUTC: true)
  var actualArrival: ParquetTimestamp  // UTC instant for cross-timezone queries
  // snippet.hide
}
// snippet.show
