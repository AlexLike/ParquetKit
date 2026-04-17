[![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2FAlexLike%2FParquetKit%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/AlexLike/ParquetKit)
[![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2FAlexLike%2FParquetKit%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/AlexLike/ParquetKit)
![CI](https://github.com/alexlike/ParquetKit/actions/workflows/ci.yml/badge.svg?label=test)

<img src="./logo.svg" alt="Logo" width="64" />

# ParquetKit

ParquetKit reads and writes [Apache Parquet](https://parquet.apache.org)™ files using Swift `Codable` structs. Annotate a struct with `@Parquet` and the macro generates the Parquet schema from your stored properties. Per-column compression and encoding can be set with `@ParquetColumn`.

```swift
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
try writer.write(SensorReading(
  stationCode: "NUUK-01", isActive: true, temperatureCelsius: -12.5,
  errorCode: nil,
  recordedAt: .init(date: Date(), unit: .microseconds, isAdjustedToUTC: true),
  region: "Greenland"
))
try writer.close()

// Read
for try await reading in try ParquetReader<SensorReading>(url: fileURL) {
  print(reading.stationCode, reading.temperatureCelsius)
}
```

## Installation

Add ParquetKit to your `Package.swift`:

```swift
dependencies: [
  .package(url: "https://github.com/alexlike/ParquetKit", from: "1.0.0"),
],
targets: [
  .target(name: "MyTarget", dependencies: ["ParquetKit"]),
]
```

Xcode users may encounter an error when compiling for the first time. To use any macros, including `@Parquet`, click the Error message and confirm the pop-up dialog.

> **Note** ParquetKit uses the [official Rust `parquet` crate](https://docs.rs/crate/parquet/latest) as a backend but ships a pre-built XCFramework, so no Rust toolchain is required when consuming the package.

## Documentation

Comprehensive API documentation and examples are available at [SwiftPackageIndex/ParquetKit](https://swiftpackageindex.com/alexlike/ParquetKit/documentation/parquetkit).

### Supported types

| Swift type                                         | Parquet type                         | Notes                                  |
| -------------------------------------------------- | ------------------------------------ | -------------------------------------- |
| `Bool`                                             | `BOOLEAN`                            |                                        |
| `Int8`                                             | `INT8`                               |                                        |
| `Int16`                                            | `INT16`                              |                                        |
| `Int32`                                            | `INT32`                              |                                        |
| `Int64`                                            | `INT64`                              |                                        |
| `UInt8`                                            | `UINT8`                              |                                        |
| `UInt16`                                           | `UINT16`                             |                                        |
| `UInt32`                                           | `UINT32`                             |                                        |
| `UInt64`                                           | `UINT64`                             |                                        |
| `Float`                                            | `FLOAT`                              |                                        |
| `Double`                                           | `DOUBLE`                             |                                        |
| `Float16`                                          | `FLOAT16`                            | macOS 11+, iOS 14+                     |
| `String`                                           | `STRING` (UTF-8)                     |                                        |
| `Data`                                             | `BYTE_ARRAY`                         |                                        |
| `UUID`                                             | `FIXED_LEN_BYTE_ARRAY(16)` or `UUID` |                                        |
| `Optional<T>`                                      | nullable column                      | any supported type                     |
| `[T]`                                              | `LIST`                               | any supported element type             |
| nested `@Parquet` struct                           | `STRUCT`                             |                                        |
| `ParquetDate`                                      | `DATE`                               | days since Unix epoch                  |
| `ParquetTimestamp` + `@ParquetTimestamp(unit:)`    | `TIMESTAMP`                          | millis/micros/nanos, UTC or wall-clock |
| `ParquetTime` + `@ParquetTime(unit:)`              | `TIME`                               | defaults to microseconds               |
| `Decimal128` + `@ParquetDecimal(precision:scale:)` | `DECIMAL`                            |                                        |
| `ParquetInterval`                                  | `INTERVAL`                           | months, days, milliseconds             |
| `Duration`                                         | `DURATION` (nanoseconds)             | Swift Concurrency `Duration`           |

### Encodings

| Encoding                | Best for                                          |
| ----------------------- | ------------------------------------------------- |
| `.plain`                | Default                                           |
| `.deltaBinaryPacked`    | Sorted or slowly-changing integers and timestamps |
| `.deltaLengthByteArray` | Variable-length strings with similar lengths      |
| `.deltaByteArray`       | Strings with shared prefixes (URLs, paths)        |
| `.rleDictionary`        | Low-cardinality string or integer columns         |

### Compression

| Codec             | Notes                               |
| ----------------- | ----------------------------------- |
| `.none`           | No compression                      |
| `.snappy`         | Default; fast                       |
| `.lz4`            | Lower ratio than Snappy; very fast  |
| `.gzip(level:)`   | Levels 1–9                          |
| `.zstd(level:)`   | Levels 1–22; high compression ratio |
| `.brotli(level:)` | Levels 0–11                         |

## Contributing

Pull requests are welcome, including small improvements or first-time contributions. :)

**Prerequisites**: [Swift](https://www.swift.org/install) (and optionally [Xcode](https://developer.apple.com/xcode/) 16+), [Rust](https://rustup.rs) (toolchain is auto-installed from `Driver/rust-toolchain.toml`)

**Available Scripts**:

```bash
# General Tools

# 1 Auto-format Swift and Rust sources
./scripts/auto-format

# 2 Run Swift and Rust tests
./scripts/run-tests

# 3 Build and preview DocC documentation in your browser
./scripts/preview-docs

# When modifying the Rust Driver

# 4 Build the Rust driver and generate the xcframework + Swift bindings
./scripts/build-xcframework

# 5 Tell Swift to use the fresh local xcframework instead of the old and prebuilt one
./scripts/use-local-binary

# 6 Check whether the Swift bindings are up-to-date
./scripts/check-bindings-sync
```

To catch formatting or binary recompilation issues before they reach CI, install the pre-commit hook once via

```bash
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

This will block commiting malformated source code or source code that references an old build of the Rust driver.

Use [Conventional Commits](https://www.conventionalcommits.org) (`feat:`, `fix:`, etc.). Commit messages drive the changelog and version bumps.

## License

Copyright 2026 Alexander Zank (unless the file header denotes otherwise)

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details and [NOTICE](NOTICE) for attributions.

## Alternatives

ParquetKit may be just perfect for your use case. However, these alternatives might too:

- [parquet-swift](https://github.com/codelynx/parquet-swift), a pure Swift implementation of the Parquet file format still under development. At the time of writing this, many encodings and compression algos are still missing.
- [SwiftArrowParquet](https://github.com/patrick-zippenfenig/SwiftArrowParquet), a basic wrapper around the Apache Arrow GLibC library.
