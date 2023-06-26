package ai.starlake.schema.model

final case class AttributeMergeStrategy(
  failOnContainerMismatch: Boolean,
  failOnAttributesEmptinessMismatch: Boolean,
  keepSourceDiffAttributesStrategy: KeepSourceDiffAttributesStrategy,
  attributePropertiesMergeStrategy: AttributeRefPropertiesMergeStrategy
)

sealed trait KeepSourceDiffAttributesStrategy
// All fields present in the existing yml schema are kept regardless of their presence in the incoming schema.
final case object KeepAllDiff extends KeepSourceDiffAttributesStrategy

// All scripted fields present in the existing yml schema are kept which means that the incoming schema override the existing one.
// If a incoming column has the same name as an existing scripted field. The latter is overriden
final case object KeepOnlyScriptDiff extends KeepSourceDiffAttributesStrategy

// Incoming schema override existing schema.
final case object DropAll extends KeepSourceDiffAttributesStrategy

/** Define which information has higher priority to merge in ref attribute property.
  */
sealed trait AttributeRefPropertiesMergeStrategy
// Attributes properties are based on the following priorities:
// RefFirst: the incoming schema overrides the existing properties
final case object RefFirst extends AttributeRefPropertiesMergeStrategy

// SourceFirst: The existing shcema in the yml file are kept intact.
final case object SourceFirst extends AttributeRefPropertiesMergeStrategy
