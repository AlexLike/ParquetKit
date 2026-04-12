// snippet.hide
import ParquetKit

// snippet.show

// snippet.modern
// Preferred: no annotation required on modern OS.
@available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *)
@Parquet
struct ImageEmbedding: Codable {
  var score: Float16
}
// snippet.end

// snippet.fallback
// Fallback for older OS targets that lack native Float16:
@Parquet
struct ImageEmbeddingLegacy: Codable {
  @ParquetFloat16
  var score: Float
}
// snippet.end
