# ``ParquetKit``

Read and write Parquet files from Swift Codable structs.

## Overview

ParquetKit reads and writes [Apache Parquet](https://parquet.apache.org)™ files using Swift `Codable` structs. Annotate a struct with `@Parquet` and the macro generates the Parquet schema from your stored properties. Per-column compression and encoding can be set with `@ParquetColumn`.

@Snippet(path: "ParquetKit/Snippets/GettingStarted")

## Supported types

| Swift type | Parquet type | Notes |
|---|---|---|
| `Bool` | `BOOLEAN` | |
| `Int8` | `INT8` | |
| `Int16` | `INT16` | |
| `Int32` | `INT32` | |
| `Int64` | `INT64` | |
| `UInt8` | `UINT8` | |
| `UInt16` | `UINT16` | |
| `UInt32` | `UINT32` | |
| `UInt64` | `UINT64` | |
| `Float` | `FLOAT` | |
| `Double` | `DOUBLE` | |
| `Float16` | `FLOAT16` | macOS 11+, iOS 14+ |
| `String` | `STRING` (UTF-8) | |
| `Data` | `BYTE_ARRAY` | |
| `UUID` | `FIXED_LEN_BYTE_ARRAY(16)` or `UUID` | |
| `Optional<T>` | nullable column | any supported type |
| `[T]` | `LIST` | any supported element type |
| nested `@Parquet` struct | `STRUCT` | |
| `ParquetDate` | `DATE` | days since Unix epoch |
| `ParquetTimestamp` + `@ParquetTimestamp(unit:)` | `TIMESTAMP` | millis/micros/nanos, UTC or wall-clock |
| `ParquetTime` + `@ParquetTime(unit:)` | `TIME` | defaults to microseconds |
| `Decimal128` + `@ParquetDecimal(precision:scale:)` | `DECIMAL` | |
| `ParquetInterval` | `INTERVAL` | months, days, milliseconds |
| `Duration` | `DURATION` (nanoseconds) | Swift Concurrency `Duration` |

## Encodings

| Encoding | Best for |
|---|---|
| `.plain` | Default |
| `.deltaBinaryPacked` | Sorted or slowly-changing integers and timestamps |
| `.deltaLengthByteArray` | Variable-length strings with similar lengths |
| `.deltaByteArray` | Strings with shared prefixes (URLs, paths) |
| `.rleDictionary` | Low-cardinality string or integer columns |

## Compression

| Codec | Notes |
|---|---|
| `.none` | No compression |
| `.snappy` | Default; fast |
| `.lz4` | Lower ratio than Snappy; very fast |
| `.gzip(level:)` | Levels 1–9 |
| `.zstd(level:)` | Levels 1–22; high compression ratio |
| `.brotli(level:)` | Levels 0–11 |

## Topics

### From struct to schema

- ``Parquet()``
- ``ParquetCodable``
- ``ParquetColumn(compression:encoding:enableDictionary:)``
- ``ParquetIgnored()``

### Reading and writing

- ``ParquetWriter``
- ``ParquetWriterConfiguration``
- ``ParquetReader``
- ``ParquetReaderConfiguration``

### Special types
- ``ParquetTimestamp``
- ``ParquetTimestamp(unit:isAdjustedToUTC:)``
- ``ParquetDate``
- ``ParquetTime``
- ``ParquetTime(unit:)``
- ``Decimal128``
- ``ParquetDecimal(precision:scale:)``
- ``ParquetInterval``
- ``ParquetFloat16()``
