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
      url: "https://github.com/AlexLike/ParquetKit/releases/download/v1.0.4/ParquetKitRustDriver.xcframework.zip",
      checksum: "817ff6635298d1e38659db36127cc03d391969f836953f7edc381b7a9e988fc8"
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
