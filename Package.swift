// swift-tools-version: 6.0

import CompilerPluginSupport
import PackageDescription

let package = Package(
  name: "ParquetKit",
  platforms: [
    .macOS(.v10_15), .iOS(.v13), .visionOS(.v1), .tvOS(.v13), .watchOS(.v9), .macCatalyst(.v13),
  ],
  products: [
    .library(
      name: "ParquetKit",
      targets: ["ParquetKit"]
    )
  ],
  dependencies: [
    .package(url: "https://github.com/swiftlang/swift-syntax", "600.0.0"..<"700.0.0"),
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),
  ],
  targets: [
    .binaryTarget(
      name: "ParquetKitRustDriver",
      url: "https://github.com/AlexLike/ParquetKit/releases/download/v1.1.1/ParquetKitRustDriver.xcframework.zip",
      checksum: "913670be5e0d42f6fa186e8bc0e33782fff5d9c35556f2b73f8b5527b2d1bd34"
    ),
    .target(
      name: "ParquetKitFFI",
      dependencies: ["ParquetKitRustDriver"]
    ),
    .macro(
      name: "ParquetKitMacros",
      dependencies: [
        .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
        .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
      ]
    ),
    .target(
      name: "ParquetKit",
      dependencies: [
        "ParquetKitFFI",
        "ParquetKitMacros",
      ]
    ),
    .testTarget(
      name: "ParquetKitTests",
      dependencies: ["ParquetKit"]
    ),
    .testTarget(
      name: "MacroExpansionTests",
      dependencies: [
        "ParquetKitMacros",
        .product(name: "SwiftSyntaxMacrosTestSupport", package: "swift-syntax"),
      ]
    ),
  ]
)
