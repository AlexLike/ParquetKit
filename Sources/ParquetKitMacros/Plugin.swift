// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import SwiftCompilerPlugin
import SwiftSyntaxMacros

@main
struct ParquetKitMacrosPlugin: CompilerPlugin {
  let providingMacros: [Macro.Type] = [
    ParquetMacro.self,
    ParquetDecimalMacro.self,
    ParquetTimestampMacro.self,
    ParquetTimeMacro.self,
    ParquetFloat16Macro.self,
    ParquetIgnoredMacro.self,
    ParquetColumnMacro.self,
  ]
}
