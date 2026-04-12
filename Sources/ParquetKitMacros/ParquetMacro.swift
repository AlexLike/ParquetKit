// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

import SwiftDiagnostics
import SwiftSyntax
import SwiftSyntaxMacros

public struct ParquetMacro: ExtensionMacro {
  public static func expansion(
    of node: AttributeSyntax,
    attachedTo declaration: some DeclGroupSyntax,
    providingExtensionsOf type: some TypeSyntaxProtocol,
    conformingTo protocols: [TypeSyntax],
    in context: some MacroExpansionContext
  ) throws -> [ExtensionDeclSyntax] {
    guard let structDecl = declaration.as(StructDeclSyntax.self) else {
      context.diagnose(
        Diagnostic(
          node: node,
          message: DiagMessage(id: "notAStruct", message: "@Parquet can only be applied to structs")
        ))
      return []
    }

    // Detect Encodable / Decodable / Codable in the inheritance clause.
    let inheritedNames = Set(
      structDecl.inheritanceClause?.inheritedTypes
        .map { $0.type.trimmedDescription } ?? [])
    let hasEncodable =
      inheritedNames.contains("Encodable") || inheritedNames.contains("Codable")
    let hasDecodable =
      inheritedNames.contains("Decodable") || inheritedNames.contains("Codable")

    if !hasEncodable && !hasDecodable {
      context.diagnose(
        Diagnostic(
          node: node,
          message: DiagMessage(
            id: "notCodable",
            message:
              "@Parquet requires the type to conform to Encodable, Decodable, or Codable")))
      return []
    }

    let conformanceList: String
    switch (hasEncodable, hasDecodable) {
    case (true, true): conformanceList = "ParquetEncodable, ParquetDecodable"
    case (true, false): conformanceList = "ParquetEncodable"
    default: conformanceList = "ParquetDecodable"
    }

    let members = structDecl.memberBlock.members

    // Resolve CodingKeys if present.
    let codingKeysMap = resolveCodingKeys(from: members)

    // Collect stored properties.
    let properties = members.compactMap { $0.decl.as(VariableDeclSyntax.self) }
      .filter { isStoredProperty($0) }

    // Warn if @ParquetColumn is used on a Decodable-only type.
    if !hasEncodable {
      for property in properties {
        if hasAttribute("ParquetColumn", in: property.attributes) {
          context.diagnose(
            Diagnostic(
              node: Syntax(property),
              message: DiagMessage(
                id: "columnOnDecodableOnly",
                message: "@ParquetColumn has no effect on a Decodable-only type",
                severity: .warning)))
        }
      }
    }

    // Generate schema entries and column configs.
    var schemaEntries: [String] = []
    var columnConfigEntries: [String] = []
    var hasErrors = false

    for property in properties {
      guard let binding = property.bindings.first,
        let pattern = binding.pattern.as(IdentifierPatternSyntax.self)
      else { continue }

      let propertyName = pattern.identifier.text
      let attributes = property.attributes

      // Check if @ParquetIgnored
      if hasAttribute("ParquetIgnored", in: attributes) {
        // Validate: must have a default value.
        if binding.initializer == nil {
          context.diagnose(
            Diagnostic(
              node: Syntax(property),
              message: DiagMessage(
                id: "ignoredNoDefault",
                message: "@ParquetIgnored property must have a default value")))
          hasErrors = true
        }
        // @ParquetColumn on @ParquetIgnored is a warning.
        if hasAttribute("ParquetColumn", in: attributes) {
          context.diagnose(
            Diagnostic(
              node: Syntax(property),
              message: DiagMessage(
                id: "columnOnIgnored",
                message: "@ParquetColumn has no effect on an @ParquetIgnored property",
                severity: .warning)))
        }
        continue
      }

      // Resolve column name.
      let columnName = codingKeysMap[propertyName] ?? propertyName

      // Resolve type.
      guard let typeAnnotation = binding.typeAnnotation?.type else { continue }
      let typeName = typeAnnotation.trimmedDescription

      // Check for @ParquetColumn on struct columns.
      if hasAttribute("ParquetColumn", in: attributes) {
        if isStructType(typeName) {
          context.diagnose(
            Diagnostic(
              node: Syntax(property),
              message: DiagMessage(
                id: "columnOnStruct",
                message:
                  "@ParquetColumn cannot be applied to a struct column; annotate the struct's own properties instead"
              )))
          hasErrors = true
        }
      }

      guard
        let schemaEntry = resolveSchemaEntry(
          typeName: typeName,
          columnName: columnName,
          attributes: attributes,
          property: property,
          context: context,
          hasErrors: &hasErrors
        )
      else {
        continue
      }

      schemaEntries.append(schemaEntry)

      // Collect @ParquetColumn configuration.
      if let columnConfig = resolveColumnConfig(
        columnName: columnName, attributes: attributes)
      {
        columnConfigEntries.append(columnConfig)
      }
    }

    if hasErrors {
      return []
    }

    let schemaBody = schemaEntries.joined(separator: ",\n      ")

    var extensionBody: String
    if hasEncodable && !columnConfigEntries.isEmpty {
      let configBody = columnConfigEntries.joined(separator: "\n      ")
      extensionBody = """
        extension \(type.trimmedDescription): \(conformanceList) {
          static var parquetSchema: [FieldSchema] {
            [
              \(schemaBody)
            ]
          }

          static var defaultWriterConfiguration: ParquetWriterConfiguration {
            var config = ParquetWriterConfiguration.default
            config.columnOverrides = [
              \(configBody)
            ]
            return config
          }
        }
        """
    } else {
      extensionBody = """
        extension \(type.trimmedDescription): \(conformanceList) {
          static var parquetSchema: [FieldSchema] {
            [
              \(schemaBody)
            ]
          }
        }
        """
    }

    let ext = try ExtensionDeclSyntax("\(raw: extensionBody)")
    return [ext]
  }
}

// MARK: - Schema Resolution

private func resolveSchemaEntry(
  typeName: String,
  columnName: String,
  attributes: AttributeListSyntax,
  property: VariableDeclSyntax,
  context: some MacroExpansionContext,
  hasErrors: inout Bool
) -> String? {
  let isOptional = typeName.hasSuffix("?")
  let baseType = isOptional ? String(typeName.dropLast()) : typeName
  let nullable = isOptional ? "true" : "false"

  // Check for array types; use the recursive helper so [[T]] works too.
  if baseType.hasPrefix("[") && baseType.hasSuffix("]") {
    let rawElement = String(baseType.dropFirst().dropLast())
    let elementEntry = resolveListElementEntry(typeName: rawElement)
    return ".list(name: \"\(columnName)\", element: \(elementEntry), nullable: \(nullable))"
  }

  // Check for special annotated types.
  if baseType == "Decimal128" {
    guard let (precision, scale) = extractDecimalParams(from: attributes) else {
      context.diagnose(
        Diagnostic(
          node: Syntax(property),
          message: DiagMessage(
            id: "decimal128NoAnnotation",
            message: "Decimal128 requires @ParquetDecimal(precision:scale:)")))
      hasErrors = true
      return nil
    }
    return
      ".primitive(name: \"\(columnName)\", type: .decimal128(precision: \(precision), scale: \(scale)), nullable: \(nullable))"
  }

  if baseType == "Decimal" {
    context.diagnose(
      Diagnostic(
        node: Syntax(property),
        message: DiagMessage(
          id: "bareDecimal", message: "Use Decimal128 with @ParquetDecimal(precision:scale:)")))
    hasErrors = true
    return nil
  }

  if baseType == "ParquetTimestamp" {
    guard let (unit, isUtc) = extractTimestampParams(from: attributes) else {
      context.diagnose(
        Diagnostic(
          node: Syntax(property),
          message: DiagMessage(
            id: "timestampNoAnnotation",
            message: "ParquetTimestamp requires @ParquetTimestamp(unit:)")))
      hasErrors = true
      return nil
    }
    let primitiveType: String
    switch (unit, isUtc) {
    case ("milliseconds", false): primitiveType = ".timestampMs"
    case ("microseconds", false): primitiveType = ".timestampUs"
    case ("nanoseconds", false): primitiveType = ".timestampNs"
    case ("milliseconds", true): primitiveType = ".timestampMsUtc"
    case ("microseconds", true): primitiveType = ".timestampUsUtc"
    case ("nanoseconds", true): primitiveType = ".timestampNsUtc"
    default: primitiveType = ".timestampUs"
    }
    return ".primitive(name: \"\(columnName)\", type: \(primitiveType), nullable: \(nullable))"
  }

  if baseType == "ParquetTime" {
    // @ParquetTime(unit:) is optional; undecorated defaults to .timeUs (backward compat).
    let primitiveType: String
    if let unit = extractTimeUnit(from: attributes) {
      switch unit {
      case "milliseconds": primitiveType = ".timeMs"
      case "microseconds": primitiveType = ".timeUs"
      case "nanoseconds": primitiveType = ".timeNs"
      default: primitiveType = ".timeUs"
      }
    } else {
      primitiveType = ".timeUs"
    }
    return ".primitive(name: \"\(columnName)\", type: \(primitiveType), nullable: \(nullable))"
  }

  if baseType == "ParquetInterval" {
    return ".primitive(name: \"\(columnName)\", type: .interval, nullable: \(nullable))"
  }

  // Reject bare Dictionary<K,V> types; they require manual parquetSchema.
  if baseType.hasPrefix("Dictionary<") || baseType.hasPrefix("[") && baseType.contains(":") {
    context.diagnose(
      Diagnostic(
        node: Syntax(property),
        message: DiagMessage(
          id: "dictionaryNotSupported",
          message:
            "Dictionary fields are not supported by the @Parquet macro. Implement parquetSchema manually and use FieldSchema.map(name:keyType:valueType:nullable:)."
        )))
    hasErrors = true
    return nil
  }

  // Float with @ParquetFloat16 → half-precision (fallback for older OS without Float16).
  if baseType == "Float" && hasAttribute("ParquetFloat16", in: attributes) {
    return ".primitive(name: \"\(columnName)\", type: .float16, nullable: \(nullable))"
  }

  // Standard type mappings.
  if let entry = resolvePrimitiveOrStructSchema(baseType, name: columnName, nullable: nullable) {
    return entry
  }

  // Unknown type; assume it's a ParquetCodable struct.
  return
    ".structType(name: \"\(columnName)\", fields: \(baseType).parquetSchema, nullable: \(nullable))"
}

/// Recursively resolves a list element type (handles `[T]`, `[T?]`, `[[T]]`, etc.)
/// and returns the `FieldSchema` factory expression for the element.
private func resolveListElementEntry(typeName: String) -> String {
  // Strip a trailing ? to determine optional element.
  let isOptional = typeName.hasSuffix("?")
  let baseType = isOptional ? String(typeName.dropLast()) : typeName
  let nullable = isOptional ? "true" : "false"

  // Primitive type (stops recursion).
  if let entry = resolvePrimitiveOrStructSchema(baseType, name: "item", nullable: nullable) {
    return entry
  }

  // Nested array; recurse one level deeper.
  if baseType.hasPrefix("[") && baseType.hasSuffix("]") {
    let innerRaw = String(baseType.dropFirst().dropLast())
    let innerEntry = resolveListElementEntry(typeName: innerRaw)
    return ".list(name: \"item\", element: \(innerEntry), nullable: \(nullable))"
  }

  // Unknown type; assume ParquetCodable struct.
  return ".structType(name: \"item\", fields: \(baseType).parquetSchema, nullable: \(nullable))"
}

private func resolvePrimitiveOrStructSchema(_ typeName: String, name: String, nullable: String)
  -> String?
{
  let primitiveMap: [String: String] = [
    "Bool": ".bool",
    "Int8": ".int8",
    "Int16": ".int16",
    "Int32": ".int32",
    "Int64": ".int64",
    "UInt8": ".uInt8",
    "UInt16": ".uInt16",
    "UInt32": ".uInt32",
    "UInt64": ".uInt64",
    "Float": ".float32",
    "Float16": ".float16",
    "Double": ".float64",
    "String": ".utf8",
    "Data": ".bytes",
    "UUID": ".fixedBytes(size: 16)",
    "ParquetDate": ".date32",
    "Duration": ".durationNs",
  ]

  if let primitiveType = primitiveMap[typeName] {
    return ".primitive(name: \"\(name)\", type: \(primitiveType), nullable: \(nullable))"
  }
  return nil
}

private func isStructType(_ typeName: String) -> Bool {
  let primitives: Set<String> = [
    "Bool", "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float", "Float16", "Double", "String", "Data", "UUID",
    "Decimal128", "ParquetTimestamp", "ParquetDate", "ParquetTime",
    "ParquetInterval", "Duration",
  ]
  let base = typeName.hasSuffix("?") ? String(typeName.dropLast()) : typeName
  return !primitives.contains(base) && !base.hasPrefix("[")
}

// MARK: - Column Configuration

private func resolveColumnConfig(
  columnName: String,
  attributes: AttributeListSyntax
) -> String? {
  guard let attr = findAttribute("ParquetColumn", in: attributes),
    let args = attr.arguments?.as(LabeledExprListSyntax.self)
  else { return nil }

  var parts: [String] = []
  for arg in args {
    guard let label = arg.label?.text else { continue }
    let value = arg.expression.trimmedDescription
    if value == "nil" { continue }
    switch label {
    case "compression":
      parts.append("compression: \(value)")
    case "encoding":
      parts.append("encoding: \(value)")
    case "enableDictionary":
      parts.append("enableDictionary: \(value)")
    default:
      break
    }
  }

  if parts.isEmpty { return nil }

  return "\"\(columnName)\": .init(\(parts.joined(separator: ", "))),"
}

// MARK: - CodingKeys Resolution

private func resolveCodingKeys(from members: MemberBlockItemListSyntax) -> [String: String] {
  var map: [String: String] = [:]

  for member in members {
    guard let enumDecl = member.decl.as(EnumDeclSyntax.self),
      enumDecl.name.text == "CodingKeys"
    else { continue }

    for caseMember in enumDecl.memberBlock.members {
      guard let caseDecl = caseMember.decl.as(EnumCaseDeclSyntax.self) else { continue }
      for element in caseDecl.elements {
        let swiftName = element.name.text
        if let rawValue = element.rawValue?.value.as(StringLiteralExprSyntax.self) {
          let stringValue =
            rawValue.segments.compactMap { $0.as(StringSegmentSyntax.self)?.content.text }.joined()
          map[swiftName] = stringValue
        }
      }
    }
  }

  return map
}

// MARK: - Attribute Helpers

private func hasAttribute(_ name: String, in attributes: AttributeListSyntax) -> Bool {
  findAttribute(name, in: attributes) != nil
}

private func findAttribute(_ name: String, in attributes: AttributeListSyntax) -> AttributeSyntax? {
  for attr in attributes {
    if let attribute = attr.as(AttributeSyntax.self),
      attribute.attributeName.trimmedDescription == name
    {
      return attribute
    }
  }
  return nil
}

private func extractDecimalParams(from attributes: AttributeListSyntax) -> (UInt8, UInt8)? {
  guard let attr = findAttribute("ParquetDecimal", in: attributes),
    let args = attr.arguments?.as(LabeledExprListSyntax.self)
  else { return nil }

  var precision: UInt8?
  var scale: UInt8?
  for arg in args {
    if arg.label?.text == "precision" {
      precision = UInt8(arg.expression.trimmedDescription)
    }
    if arg.label?.text == "scale" {
      scale = UInt8(arg.expression.trimmedDescription)
    }
  }
  guard let p = precision, let s = scale else { return nil }
  return (p, s)
}

/// Returns (unit, isAdjustedToUTC) extracted from a @ParquetTimestamp annotation.
private func extractTimestampParams(from attributes: AttributeListSyntax) -> (String, Bool)? {
  guard let attr = findAttribute("ParquetTimestamp", in: attributes),
    let args = attr.arguments?.as(LabeledExprListSyntax.self)
  else { return nil }

  var unit: String?
  var isUtc = false
  for arg in args {
    switch arg.label?.text {
    case "unit":
      let expr = arg.expression.trimmedDescription
      unit = expr.hasPrefix(".") ? String(expr.dropFirst()) : expr
    case "isAdjustedToUTC":
      isUtc = arg.expression.trimmedDescription == "true"
    default:
      break
    }
  }
  guard let u = unit else { return nil }
  return (u, isUtc)
}

/// Returns the unit string extracted from a @ParquetTime annotation, or nil if absent.
private func extractTimeUnit(from attributes: AttributeListSyntax) -> String? {
  guard let attr = findAttribute("ParquetTime", in: attributes),
    let args = attr.arguments?.as(LabeledExprListSyntax.self)
  else { return nil }

  for arg in args {
    if arg.label?.text == "unit" {
      let expr = arg.expression.trimmedDescription
      return expr.hasPrefix(".") ? String(expr.dropFirst()) : expr
    }
  }
  return nil
}

// MARK: - Property Helpers

private func isStoredProperty(_ variable: VariableDeclSyntax) -> Bool {
  // Computed properties have accessor blocks with get/set.
  guard let binding = variable.bindings.first else { return false }
  if let accessor = binding.accessorBlock {
    // If it has a code block accessor (get { ... }), it's computed.
    if accessor.accessors.is(AccessorDeclListSyntax.self) {
      return false
    }
  }
  // Static properties are not stored instance properties.
  if variable.modifiers.contains(where: { $0.name.text == "static" }) {
    return false
  }
  return true
}

// MARK: - Diagnostic Message

struct DiagMessage: DiagnosticMessage {
  let id: String
  let message: String
  var severity: DiagnosticSeverity = .error

  var diagnosticID: MessageID {
    MessageID(domain: "ParquetKit", id: id)
  }
}
