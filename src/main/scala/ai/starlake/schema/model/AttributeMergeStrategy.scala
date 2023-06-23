package ai.starlake.schema.model

final case class AttributeMergeStrategy(
  failOnContainerMismatch: Boolean,
  failOnAttributesEmptinessMismatch: Boolean,
  keepSourceDiffAttributesStrategy: KeepSourceDiffAttributesStrategy,
  attributePropertiesMergeStrategy: AttributeRefPropertiesMergeStrategy
)

sealed trait KeepSourceDiffAttributesStrategy
final case object KeepAllDiff extends KeepSourceDiffAttributesStrategy

final case object KeepOnlyScriptDiff extends KeepSourceDiffAttributesStrategy

final case object DropAll extends KeepSourceDiffAttributesStrategy

/** Define which information has higher priority to merge in ref attribute property.
  */
sealed trait AttributeRefPropertiesMergeStrategy
final case object RefFirst extends AttributeRefPropertiesMergeStrategy
final case object SourceFirst extends AttributeRefPropertiesMergeStrategy
