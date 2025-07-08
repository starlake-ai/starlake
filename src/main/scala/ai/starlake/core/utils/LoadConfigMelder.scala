package ai.starlake.core.utils

import ai.starlake.schema.model
import ai.starlake.schema.model.{Attribute, DomainInfo, SchemaInfo, TransformInput}

/** Represents the precedence level for configuration derived from extracted metadata. This object
  * is used within configuration settings to specify that certain values should be taken from the
  * extracted one rather than Current.
  */
sealed trait ConfigPrecedence
case object Extract extends ConfigPrecedence
case object Current extends ConfigPrecedence

/** Represents a strategy for handling conflicts and combining attributes when merging schemas
  * during domain or table melding processes. This trait is used to specify how attributes from an
  * extracted schema and an existing schema are unified.
  *
  * Implementations of this trait specify different approaches for resolving attribute conflicts:
  *   - `KeepCurrentScript`: Retains current script-defined attributes. Any attributes from the
  *     extracted schema that are not present in the current schema are dropped.
  *   - `KeepExtract`: Retains attributes from the extracted schema only. Existing attributes not
  *     present in the extracted schema are dropped.
  *   - `KeepAll`: Retains attributes from both schemas, merging them on conflict.
  *
  * Typical use cases involve schema reconciliation tasks where schemas need to be combined, such as
  * when consolidating schema definitions from multiple sources or when updating schemas in a
  * metadata repository.
  */
sealed trait AttributeUnionStrategy
case object KeepCurrentScript extends AttributeUnionStrategy
case object KeepExtract extends AttributeUnionStrategy
case object KeepAll extends AttributeUnionStrategy

/** Configuration class to determine how various attributes of a table should be handled during the
  * melding process by applying precedence rules.
  *
  * Each field represents a specific attribute of a table or attribute-level metadata, and its value
  * determines the precedence to resolve conflicts when merging information from extracted sources
  * and current schemas.
  *
  * @param required
  *   Precedence for handling the "required" attribute of table fields.
  * @param privacy
  *   Precedence for handling privacy-related metadata for the fields.
  * @param comment
  *   Precedence for handling any comments or documentation for the fields.
  * @param rename
  *   Precedence for handling renaming of attributes in the schema.
  * @param metricType
  *   Precedence for handling metric type classification of attributes.
  * @param position
  *   Precedence for handling positional information of attributes.
  * @param default
  *   Precedence for handling default values associated with attributes.
  * @param tags
  *   Precedence for handling tags or labels associated with attributes.
  * @param trim
  *   Precedence for handling trim-related operations for stringfields.
  * @param foreignKey
  *   Precedence for handling foreign key relationships.
  * @param ignore
  *   Precedence for determining if a field should be ignored.
  * @param accessPolicy
  *   Precedence for handling access policies for attributes.
  * @param sample
  *   Precedence for handling sample data of the attributes.
  */
case class TableAttributeMelderConfig(
  required: ConfigPrecedence = Extract,
  privacy: ConfigPrecedence = Current,
  comment: ConfigPrecedence = Extract,
  rename: ConfigPrecedence = Extract,
  metricType: ConfigPrecedence = Current,
  position: ConfigPrecedence = Extract,
  default: ConfigPrecedence = Current,
  tags: ConfigPrecedence = Current,
  trim: ConfigPrecedence = Current,
  foreignKey: ConfigPrecedence = Extract,
  ignore: ConfigPrecedence = Current,
  accessPolicy: ConfigPrecedence = Current,
  sample: ConfigPrecedence = Extract
)

case class TableMelderConfig(
  pattern: ConfigPrecedence = Current,
  rename: ConfigPrecedence = Extract,
  comment: ConfigPrecedence = Extract,
  metadata: ConfigPrecedence = Current,
  presql: ConfigPrecedence = Current,
  postsql: ConfigPrecedence = Current,
  tags: ConfigPrecedence = Current,
  rls: ConfigPrecedence = Current,
  expectations: ConfigPrecedence = Current,
  primaryKeys: ConfigPrecedence = Extract,
  acl: ConfigPrecedence = Current,
  sample: ConfigPrecedence = Extract,
  patternSample: ConfigPrecedence = Current,
  filter: ConfigPrecedence = Current
)

case class DomainMelderConfig(
  comment: ConfigPrecedence =
    Extract, // Comments are usually extracted, therefore they have higher precedence
  tags: ConfigPrecedence = Current, // tags are usually set by the user
  rename: ConfigPrecedence = Current, // rename is usually set by the user
  database: ConfigPrecedence =
    Current, // database is usually set by the user since it indicates the target database, not the database where we extract schema from
  metadata: ConfigPrecedence = Current // metadata usually get modified by the user
)

class LoadConfigMelder() {

  private def selectValue[T, U](extractedDomain: T, currentDomain: Option[T])(
    extractor: T => U,
    precedence: ConfigPrecedence
  ): U = {
    def isFilled(value: Any, depth: Int = 0): Boolean = {
      value match {
        case l: Iterable[_] =>
          l.nonEmpty && (depth == 0 && l.forall(isFilled(_, depth + 1)) || depth > 0)
        case o: Option[_] =>
          o.isDefined && (depth == 0 && o.forall(isFilled(_, depth + 1)) || depth > 0)
        case s: String               => s.trim.nonEmpty
        case t: model.TransformInput => t != TransformInput.None
        case _                       => true
      }
    }
    precedence match {
      case Extract =>
        val extractedValue = extractor(extractedDomain)
        if (isFilled(extractedValue)) {
          extractedValue
        } else {
          currentDomain.map(extractor).filter(isFilled(_)).getOrElse(extractedValue)
        }
      case Current =>
        currentDomain.map(extractor).filter(isFilled(_)).getOrElse(extractor(extractedDomain))
    }
  }

  def meldDomainRecursively(
    domainMelderConfig: DomainMelderConfig,
    tableMelderConfig: TableMelderConfig,
    tableAttributeMelderConfig: TableAttributeMelderConfig,
    attributeUnionStrategy: AttributeUnionStrategy,
    extractedDomain: DomainInfo,
    currentDomain: Option[DomainInfo]
  ): DomainInfo = {

    val meldedDomain = meldDomain(domainMelderConfig, extractedDomain, currentDomain)
    val currentTablesMap =
      currentDomain.map(_.tables).getOrElse(List.empty).map(t => t.name -> t).toMap
    val meldedTables = meldedDomain.tables.map { extractedTable =>
      meldTable(
        tableMelderConfig,
        tableAttributeMelderConfig,
        attributeUnionStrategy,
        extractedTable,
        currentTablesMap.get(extractedTable.name)
      )
    }
    meldedDomain.copy(tables = meldedTables)
  }

  def meldDomain(
    melderConfig: DomainMelderConfig,
    extractedDomain: DomainInfo,
    currentDomain: Option[DomainInfo]
  ): DomainInfo = {
    extractedDomain.copy(
      comment = selectValue(extractedDomain, currentDomain)(_.comment, melderConfig.comment),
      tags = selectValue(extractedDomain, currentDomain)(_.tags, melderConfig.tags),
      rename = selectValue(extractedDomain, currentDomain)(_.rename, melderConfig.rename),
      database = selectValue(extractedDomain, currentDomain)(_.database, melderConfig.database),
      metadata = selectValue(extractedDomain, currentDomain)(_.metadata, melderConfig.metadata)
    )
    // handle tables
  }

  def meldTable(
    tableMelderConfig: TableMelderConfig,
    attributeMelderConfig: TableAttributeMelderConfig,
    attributeUnionStrategy: AttributeUnionStrategy,
    extractedSchema: SchemaInfo,
    currentSchema: Option[SchemaInfo]
  ): SchemaInfo = {
    extractedSchema.copy(
      pattern = selectValue(extractedSchema, currentSchema)(_.pattern, tableMelderConfig.pattern),
      metadata =
        selectValue(extractedSchema, currentSchema)(_.metadata, tableMelderConfig.metadata),
      comment = selectValue(extractedSchema, currentSchema)(_.comment, tableMelderConfig.comment),
      presql = selectValue(extractedSchema, currentSchema)(_.presql, tableMelderConfig.presql),
      postsql = selectValue(extractedSchema, currentSchema)(_.postsql, tableMelderConfig.postsql),
      tags = selectValue(extractedSchema, currentSchema)(_.tags, tableMelderConfig.tags),
      rls = selectValue(extractedSchema, currentSchema)(_.rls, tableMelderConfig.rls),
      expectations =
        selectValue(extractedSchema, currentSchema)(_.expectations, tableMelderConfig.expectations),
      primaryKey =
        selectValue(extractedSchema, currentSchema)(_.primaryKey, tableMelderConfig.primaryKeys),
      acl = selectValue(extractedSchema, currentSchema)(_.acl, tableMelderConfig.acl),
      rename = selectValue(extractedSchema, currentSchema)(_.rename, tableMelderConfig.rename),
      sample = selectValue(extractedSchema, currentSchema)(_.sample, tableMelderConfig.sample),
      filter = selectValue(extractedSchema, currentSchema)(_.filter, tableMelderConfig.filter),
      patternSample = selectValue(extractedSchema, currentSchema)(
        _.patternSample,
        tableMelderConfig.patternSample
      ),
      attributes = meldTableAttributes(
        attributeMelderConfig,
        attributeUnionStrategy,
        extractedSchema.attributes,
        currentSchema.map(_.attributes)
      )
    )
  }

  def meldTableAttributes(
    melderConfig: TableAttributeMelderConfig,
    attributeUnionStrategy: AttributeUnionStrategy,
    extractedAttributes: List[Attribute],
    currentAttributes: Option[List[Attribute]]
  ): List[Attribute] = {
    val currentAttributesScope: List[Attribute] = attributeUnionStrategy match {
      case KeepCurrentScript =>
        val extractedAttributesNames = extractedAttributes.map(_.name)
        currentAttributes
          .map(_.filter(a => extractedAttributesNames.contains(a.name) || a.script.isDefined))
          .getOrElse(List.empty)
      case KeepExtract =>
        val extractedAttributesNames = extractedAttributes.map(_.name)
        currentAttributes
          .map(_.filter(a => extractedAttributesNames.contains(a.name)))
          .getOrElse(List.empty)
      case KeepAll =>
        currentAttributes.getOrElse(List.empty)
    }
    val currentAttributesMap = currentAttributesScope.map(a => a.name -> a).toMap
    val mergedExtractedAttributes: List[Attribute] = extractedAttributes.map { extractedAttr =>
      {
        val currentAttr = currentAttributesMap.get(extractedAttr.name)
        extractedAttr.copy(
          required = selectValue(extractedAttr, currentAttr)(_.required, melderConfig.required),
          privacy = selectValue(extractedAttr, currentAttr)(_.privacy, melderConfig.privacy),
          comment = selectValue(extractedAttr, currentAttr)(_.comment, melderConfig.comment),
          rename = selectValue(extractedAttr, currentAttr)(_.rename, melderConfig.rename),
          metricType =
            selectValue(extractedAttr, currentAttr)(_.metricType, melderConfig.metricType),
          attributes = meldTableAttributes(
            melderConfig,
            attributeUnionStrategy,
            extractedAttr.attributes,
            currentAttr.map(_.attributes)
          ),
          position = selectValue(extractedAttr, currentAttr)(_.position, melderConfig.position),
          default = selectValue(extractedAttr, currentAttr)(_.default, melderConfig.default),
          tags = selectValue(extractedAttr, currentAttr)(_.tags, melderConfig.tags),
          trim = selectValue(extractedAttr, currentAttr)(_.trim, melderConfig.trim),
          foreignKey =
            selectValue(extractedAttr, currentAttr)(_.foreignKey, melderConfig.foreignKey),
          ignore = selectValue(extractedAttr, currentAttr)(_.ignore, melderConfig.ignore),
          accessPolicy =
            selectValue(extractedAttr, currentAttr)(_.accessPolicy, melderConfig.accessPolicy),
          sample = selectValue(extractedAttr, currentAttr)(_.sample, melderConfig.sample)
        )
      }
    }
    val mergedExtractedAttrNames = mergedExtractedAttributes.map(_.name)
    mergedExtractedAttributes ++ currentAttributesScope.filter(a =>
      !mergedExtractedAttrNames.contains(a.name)
    )
  }
}
