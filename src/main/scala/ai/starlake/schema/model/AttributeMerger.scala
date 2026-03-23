package ai.starlake.schema.model

import ai.starlake.schema.handlers.SchemaHandler

import scala.annotation.tailrec

/** Attribute merging logic extracted from TableAttribute companion object.
  */
object AttributeMerger {

  /** Compare refAttr with sourceAttr and reject if emptiness of attributes is not the same.
    */
  private def checkAttributesEmptinessMismatch(
    refAttr: TableAttribute,
    sourceAttr: TableAttribute
  ): Unit = {
    if (refAttr.attributes.isEmpty != sourceAttr.attributes.isEmpty)
      assert(
        refAttr.attributes.isEmpty == sourceAttr.attributes.isEmpty,
        s"attribute with name ${sourceAttr.name}: container type mismatch"
      )
  }

  /** Compare refAttr with sourceAttr and reject if container type is not the same. Furthermore, if
    * it's an array type, check that nested element are the same container type as well recursively.
    */
  private def checkContainerMismatch(refAttr: TableAttribute, sourceAttr: TableAttribute)(implicit
    schemaHandler: SchemaHandler
  ): Unit = {
    @tailrec
    def matchContainerType(
      refDataType: StarlakeDataType,
      sourceDataType: StarlakeDataType
    ): Unit = {
      (refDataType, sourceDataType) match {
        case (_: StarlakeDataType.SLArray, _) | (_, _: StarlakeDataType.SLArray) =>
          assert(
            refDataType.getClass.getSimpleName == sourceDataType.getClass.getSimpleName,
            s"attribute with name ${sourceAttr.name} is not in array in both schema"
          )
        case (_: StarlakeDataType.SLStruct, _) =>
          assert(
            refDataType.getClass.getSimpleName == sourceDataType.getClass.getSimpleName,
            s"attribute with name ${sourceAttr.name} found with type '${sourceAttr.`type`}' where type '${refAttr.`type`}' is expected"
          )
        case _ =>
      }
      (refDataType, sourceDataType) match {
        case (StarlakeDataType.SLArray(refElem), StarlakeDataType.SLArray(srcElem)) =>
          matchContainerType(refElem, srcElem)
        case _ =>
      }
    }

    val refAttrDataType = refAttr.primitiveSLType(schemaHandler)
    val sourceAttrDataType = sourceAttr.primitiveSLType(schemaHandler)
    matchContainerType(refAttrDataType, sourceAttrDataType)
  }

  private def merge(
    refAttr: TableAttribute,
    sourceAttr: TableAttribute,
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit schemaHandler: SchemaHandler): TableAttribute = {
    if (attributeMergeStrategy.failOnContainerMismatch) {
      checkContainerMismatch(refAttr, sourceAttr)
    }
    if (attributeMergeStrategy.failOnAttributesEmptinessMismatch) {
      checkAttributesEmptinessMismatch(refAttr, sourceAttr)
    }
    val refAttrDataType = refAttr.primitiveSLType(schemaHandler)
    val sourceAttrDataType = sourceAttr.primitiveSLType(schemaHandler)
    val attributes = mergeContainerAttributes(
      refAttr,
      sourceAttr,
      attributeMergeStrategy,
      refAttrDataType,
      sourceAttrDataType
    )
    val (referenceSource, fallbackSource) =
      attributeMergeStrategy.attributePropertiesMergeStrategy match {
        case RefFirst    => (refAttr, sourceAttr)
        case SourceFirst => (sourceAttr, refAttr)
      }
    TableAttribute(
      name = referenceSource.name,
      `type` = referenceSource.`type`,
      array = referenceSource.array.orElse(fallbackSource.array),
      required = referenceSource.required.orElse(fallbackSource.required),
      privacy =
        if (
          referenceSource.privacy.isDefined && !referenceSource.privacy
            .contains(TransformInput.None)
        ) referenceSource.privacy
        else fallbackSource.privacy,
      comment = referenceSource.comment.orElse(fallbackSource.comment),
      rename = referenceSource.rename.orElse(fallbackSource.rename),
      metricType = referenceSource.metricType.orElse(fallbackSource.metricType),
      attributes = attributes,
      position = referenceSource.position.orElse(fallbackSource.position),
      default = referenceSource.default.orElse(fallbackSource.default),
      tags = if (referenceSource.tags.nonEmpty) referenceSource.tags else fallbackSource.tags,
      trim = referenceSource.trim.orElse(fallbackSource.trim),
      script = referenceSource.script.orElse(fallbackSource.script),
      foreignKey = referenceSource.foreignKey.orElse(fallbackSource.foreignKey),
      ignore = referenceSource.ignore.orElse(fallbackSource.ignore),
      accessPolicy = referenceSource.accessPolicy.orElse(fallbackSource.accessPolicy)
    )
  }

  private def mergeContainerAttributes(
    refAttr: TableAttribute,
    sourceAttr: TableAttribute,
    attributeMergeStrategy: AttributeMergeStrategy,
    refAttrDataType: StarlakeDataType,
    sourceAttrDataType: StarlakeDataType
  )(implicit schemaHandler: SchemaHandler) = {
    val attributes = (refAttrDataType, sourceAttrDataType) match {
      case (_: StarlakeDataType.SLStruct, _: StarlakeDataType.SLStruct) =>
        mergeAll(refAttr.attributes, sourceAttr.attributes, attributeMergeStrategy)
      case (_: StarlakeDataType.SLStruct, _) =>
        mergeAll(refAttr.attributes, Nil, attributeMergeStrategy)
      case (StarlakeDataType.SLArray(refElem), StarlakeDataType.SLArray(srcElem)) =>
        (refElem, srcElem) match {
          case (_: StarlakeDataType.SLStruct, _: StarlakeDataType.SLStruct) =>
            mergeAll(refAttr.attributes, sourceAttr.attributes, attributeMergeStrategy)
          case (_: StarlakeDataType.SLStruct, _) =>
            mergeAll(refAttr.attributes, Nil, attributeMergeStrategy)
          case _ =>
            Nil
        }
      case _ =>
        Nil
    }
    attributes
  }

  def mergeAll(
    refAttributes: List[TableAttribute],
    sourceAttributes: List[TableAttribute],
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit schemaHandler: SchemaHandler): List[TableAttribute] = {
    val sourceAttrsByName = sourceAttributes.map(a => a.name -> a).toMap
    val refAttrsByName = refAttributes.map(a => a.name -> a).toMap
    val mergedFromRef = refAttributes.map { refAttr =>
      sourceAttrsByName.get(refAttr.name) match {
        case Some(sourceAttr) =>
          merge(refAttr, sourceAttr, attributeMergeStrategy)
        case None =>
          refAttr
      }
    }
    val newFromSource = sourceAttributes.filterNot(a => refAttrsByName.contains(a.name))
    mergedFromRef ++ newFromSource
  }
}
