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

## API Docs

Comprehensive API documentation and examples are available at [swiftpackageindex.com/alexlike/ParquetKit](https://swiftpackageindex.com/alexlike/ParquetKit/documentation/parquetkit).

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

> **Note** ParquetKit uses the [official Rust `parquet` crate](https://docs.rs/crate/parquet/latest) as a backend but ships a pre-built XCFramework, so no Rust toolchain is required when consuming the package.

## Contributing

Pull requests are welcome, including small improvements or first-time contributions. :)

**Prerequisites**: [Swift](https://www.swift.org/install) (and optionally [Xcode](https://developer.apple.com/xcode/) 16+), [Rust](https://rustup.rs) (toolchain is auto-installed from `Driver/rust-toolchain.toml`)

**Available Scripts**:

```bash
# Build the Rust driver and generate the xcframework + Swift bindings
./scripts/build-xcframework

# Auto-format Swift and Rust sources
./scripts/auto-format

# Run Swift and Rust tests
./scripts/run-tests
```

To catch formatting issues before they reach CI, install the pre-commit hook once via

```bash
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

This will block commiting malformated source code.

Use [Conventional Commits](https://www.conventionalcommits.org) (`feat:`, `fix:`, etc.). Commit messages drive the changelog and version bumps.

If you change the Rust interface, re-run step 1 and commit the updated `Sources/ParquetKitFFI/parquet_swift.swift` alongside your changes.

## License

Copyright 2026 Alexander Zank (unless the file header denotes otherwise)

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details and [NOTICE](NOTICE) for attributions.

## Alternatives

ParquetKit may be just perfect for your use case. However, these alternatives might too:

- [parquet-swift](https://github.com/codelynx/parquet-swift), a pure Swift implementation of the Parquet file format still under development. At the time of writing this, many encodings and compression algos are still missing.
- [SwiftArrowParquet](https://github.com/patrick-zippenfenig/SwiftArrowParquet), a basic wrapper around the Apache Arrow GLibC library.
