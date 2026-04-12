import Foundation
// Construct a Decimal128 from a Swift Decimal and convert it back.
// snippet.hide
import ParquetKit

func example() throws {
  // snippet.show
  // Scale 12.50 to the unscaled integer 1250 (precision 10, scale 2):
  let price = try Decimal128(from: Decimal(string: "12.50")!, scale: 2)

  // Convert back for display or arithmetic:
  let amount = price.toDecimal(scale: 2)  // → 12.50
  // snippet.hide
  _ = amount
}
// snippet.show
