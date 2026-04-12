// snippet.hide
import ParquetKit

// snippet.show

@Parquet
struct Subscription: Codable {
  var billingCycle: ParquetInterval
}

// A monthly billing cycle with no extra days or milliseconds:
let monthly = ParquetInterval(months: 1, days: 0, milliseconds: 0)
// A 14-day trial period:
let trial = ParquetInterval(months: 0, days: 14, milliseconds: 0)
