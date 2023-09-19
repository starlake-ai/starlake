/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.schema.model

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Schema.{SL_INTERNAL_TABLE, SL_TARGET_TABLE}
import ai.starlake.schema.model.Severity._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.Utils
import ai.starlake.utils.conversion.BigQueryUtils
import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.cloud.bigquery.{Schema => BQSchema}
import org.apache.spark.sql.types._

import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.Try

/** Dataset Schema
  *
  * @param name
  *   : Schema name, must be unique among all the schemas belonging to the same domain. Will become
  *   the hive table name On Premise or BigQuery Table name on GCP.
  * @param pattern
  *   : filename pattern to which this schema must be applied. This instructs the framework to use
  *   this schema to parse any file with a filename that match this pattern.
  * @param attributes
  *   : Attributes parsing rules. See :ref:`attribute_concept`
  * @param metadata
  *   : Dataset metadata See :ref:`metadata_concept`
  * @param comment
  *   : free text
  * @param presql
  *   : Reserved for future use.
  * @param postsql
  *   : We use this attribute to execute sql queries before writing the final dataFrame after
  *   ingestion
  * @param tags
  *   : Set of string to attach to this Schema
  * @param rls
  *   : Experimental. Row level security to this to this schema. See :ref:`rowlevelsecurity_concept`
  * @param filter
  *   allow accepted data to be filtered out before sinking it and column renamed. Only apply to
  *   spark engine.
  */
case class Schema(
  name: String,
  pattern: Pattern,
  attributes: List[Attribute],
  metadata: Option[Metadata],
  merge: Option[MergeOptions],
  comment: Option[String],
  presql: List[String] = Nil,
  postsql: List[String] = Nil,
  tags: Set[String] = Set.empty,
  rls: List[RowLevelSecurity] = Nil,
  expectations: Map[String, String] = Map.empty,
  primaryKey: List[String] = Nil,
  acl: List[AccessControlEntry] = Nil,
  rename: Option[String] = None,
  sample: Option[String] = None,
  filter: Option[String] = None,
  patternSample: Option[String] = None
) extends Named {
  def this() = this(
    "",
    Pattern.compile("."),
    Nil,
    None,
    None,
    None
  ) // Should never be called. Here for Jackson deserialization only

  def containsRepeatedOrNestedFields(): Boolean = {
    attributes.exists(_.isNestedOrRepeatedField())
  }

  def ddlMapping(datawarehouse: String, schemaHandler: SchemaHandler): List[DDLField] = {
    attributes.map { attribute =>
      val isPrimaryKey = primaryKey.contains(attribute.name)
      attribute.ddlMapping(isPrimaryKey, datawarehouse, schemaHandler)
    }
  }

  @JsonIgnore
  def isFlat(): Boolean = {
    !attributes.exists(_.attributes.nonEmpty)
  }

  /** @return
    *   renamed column if defined, source name otherwise
    */
  @JsonIgnore
  lazy val finalName: String = rename.getOrElse(name)

  @JsonIgnore
  lazy val attributesWithoutScriptedFields: List[Attribute] = attributes.filter(_.script.isEmpty)

  /** @return
    *   Are the parittions columns defined in the metadata valid column names
    */
  def validatePartitionColumns(): Boolean = {
    metadata.forall(
      _.getPartitionAttributes().forall(
        attributes
          .map(_.getFinalName())
          .union(Metadata.CometPartitionColumns)
          .contains
      )
    )
  }

  def scriptAndTransformAttributes(): List[Attribute] = {
    attributes.filter { attribute =>
      !attribute.isIgnore() && (attribute.script.nonEmpty || attribute.transform.nonEmpty)
    }
  }

  def exceptIgnoreScriptAndTransformAttributes(): List[Attribute] = {
    attributes.filter { attribute =>
      !attribute.isIgnore() && attribute.script.isEmpty && attribute.transform.isEmpty
    }
  }

  def ignoredAttributes(): List[Attribute] = {
    attributes.filter { attribute =>
      attribute.isIgnore()
    }
  }

  def hasTransformOrIgnoreOrScriptColumns(): Boolean = {
    attributes.count(attr => attr.isIgnore() || attr.script.nonEmpty || attr.transform.nonEmpty) > 0
  }

  /** This Schema as a Spark Catalyst Schema
    *
    * @return
    *   Spark Catalyst Schema
    */
  def sourceSparkSchema(schemaHandler: SchemaHandler): StructType = {
    val fields = attributes.map { attr =>
      StructField(attr.name, attr.sparkType(schemaHandler), !attr.required)
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  /** This Schema as a Spark Catalyst Schema, with renamed attributes
    *
    * @return
    *   Spark Catalyst Schema
    */
  def sparkSchemaFinal(schemaHandler: SchemaHandler): StructType =
    sparkSchemaWithCondition(schemaHandler, !_.isIgnore())

  private def sparkSchemaWithCondition(
    schemaHandler: SchemaHandler,
    p: Attribute => Boolean
  ): StructType = {
    val fields = attributes filter p map { attr =>
      val structField = StructField(
        attr.getFinalName(),
        attr.sparkType(schemaHandler),
        if (attr.script.isDefined) true else !attr.required
      )
      attr.comment.map(structField.withComment).getOrElse(structField)
    }
    StructType(fields)
  }

  /** This Schema as a Spark Catalyst Schema, without scripted fields
    *
    * @return
    *   Spark Catalyst Schema
    */
  def sparkSchemaWithoutScriptedFields(schemaHandler: SchemaHandler): StructType = {
    val fields = attributes.filter(_.script.isEmpty).map { attr =>
      StructField(attr.name, attr.sparkType(schemaHandler), !attr.required)
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  def sparkSchemaUntypedEpochWithoutScriptedFields(schemaHandler: SchemaHandler): StructType = {
    val fields = attributesWithoutScriptedFields.map { attr =>
      val sparkType = attr.`type`(schemaHandler).fold(attr.sparkType(schemaHandler)) { tpe =>
        (tpe.primitiveType, tpe.pattern) match {
          case (PrimitiveType.timestamp, "epoch_second") => LongType
          case (PrimitiveType.timestamp, "epoch_milli")  => LongType
          case (PrimitiveType.date, _)                   => StringType
          case (_, _)                                    => attr.sparkType(schemaHandler)
        }
      }
      StructField(attr.name, sparkType, !attr.required)
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  def sparkSchemaWithoutScriptedFieldsWithInputFileName(
    schemaHandler: SchemaHandler
  ): StructType = {
    sparkSchemaWithoutScriptedFields(schemaHandler)
      .add(StructField(CometColumns.cometInputFileNameColumn, StringType))
  }

  def sparkSchemaWithoutIgnoreAndScript(schemaHandler: SchemaHandler): StructType =
    sparkSchemaWithCondition(schemaHandler, attr => !attr.isIgnore() && attr.script.isEmpty)

  def sparkSchemaWithoutIgnore(schemaHandler: SchemaHandler): StructType =
    sparkSchemaWithCondition(schemaHandler, attr => !attr.isIgnore())

  def sparkSchemaWithIgnoreAndScript(schemaHandler: SchemaHandler): StructType =
    sparkSchemaWithCondition(schemaHandler, _ => true)

  def bqSchemaFinal(schemaHandler: SchemaHandler): BQSchema = {
    BigQueryUtils.bqSchema(sparkSchemaFinal(schemaHandler))
  }

  def bqSchemaWithoutIgnoreAndScript(schemaHandler: SchemaHandler): BQSchema = {
    BigQueryUtils.bqSchema(sparkSchemaWithoutIgnoreAndScript(schemaHandler))
  }

  def bqSchemaWithoutIgnore(schemaHandler: SchemaHandler): BQSchema = {
    BigQueryUtils.bqSchema(sparkSchemaWithoutIgnore(schemaHandler))
  }

  def bqSchemaWithIgnoreAndScript(schemaHandler: SchemaHandler): BQSchema = {
    BigQueryUtils.bqSchema(sparkSchemaWithIgnoreAndScript(schemaHandler))
  }

  /** return the list of renamed attributes
    *
    * @return
    *   list of tuples (oldname, newname)
    */
  def renamedAttributes(): List[(String, String)] = {
    attributes.filter(attr => attr.name != attr.getFinalName()).map { attr =>
      (attr.name, attr.getFinalName())
    }
  }

  def finalAttributeNames(): List[String] =
    attributes.filterNot(_.isIgnore()).map(attr => attr.getFinalName())

  /** Check attribute definition correctness :
    *   - schema name should be a valid table identifier
    *   - attribute name should be a valid Hive column identifier
    *   - attribute name can occur only once in the schema
    *
    * @return
    *   error list or true
    */
  def checkValidity(
    domainMetaData: Option[Metadata],
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.MutableList[ValidationMessage] = mutable.MutableList.empty
    val forceTablePrefixRegex = settings.appConfig.forceTablePattern.r
    if (!forceTablePrefixRegex.pattern.matcher(name).matches())
      errorList += ValidationMessage(
        Error,
        "Table",
        s"name: Table with name $name should respect the pattern ${forceTablePrefixRegex.regex}"
      )

    metadata.foreach { metadata =>
      for (errors <- metadata.checkValidity(schemaHandler).left) {
        errorList ++= errors
      }
    }

    attributes.foreach { attribute =>
      for (errors <- attribute.checkValidity(schemaHandler).left) {
        errorList ++= errors
      }
    }

    val firstScriptedFiedlIndex = attributes.indexWhere(_.script.isDefined)
    val lastNonScriptedFiedlIndex = attributes.lastIndexWhere(_.script.isEmpty)
    if (firstScriptedFiedlIndex >= 0 && firstScriptedFiedlIndex < lastNonScriptedFiedlIndex) {
      errorList +=
        ValidationMessage(
          Error,
          "Table attributes",
          s"""Scripted fields can only appear at the end of the schema. Found scripted field at position $firstScriptedFiedlIndex and non scripted field at position $lastNonScriptedFiedlIndex""".stripMargin
        )
    }

    val duplicateErrorMessage =
      "%s is defined %d times. An attribute can only be defined once."
    for (
      errors <- Utils
        .duplicates("Table attribute name", attributes.map(_.name), duplicateErrorMessage)
        .left
    ) {
      errorList ++= errors
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def discreteAttrs(schemaHandler: SchemaHandler): List[Attribute] =
    attributes.filter(_.getMetricType(schemaHandler) == MetricType.DISCRETE)

  def continuousAttrs(schemaHandler: SchemaHandler): List[Attribute] =
    attributes.filter(_.getMetricType(schemaHandler) == MetricType.CONTINUOUS)

  def esMapping(
    template: Option[String],
    domainName: String,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): String = {
    val attrs = attributes.map(_.indexMapping(schemaHandler)).mkString(",")
    val properties =
      s"""
         |"properties": {
         |$attrs
         |}""".stripMargin

    val tse = Map(
      "properties" -> properties,
      "attributes" -> attrs,
      "domain"     -> domainName.toLowerCase,
      "schema"     -> finalName.toLowerCase
    )

    template
      .getOrElse {
        """
         |{
         |  "index_patterns": ["${domain}.${schema}", "${domain}.${schema}-*"],
         |  "settings": {
         |    "number_of_shards": "1",
         |    "number_of_replicas": "0"
         |  },
         |  "mappings": {
         |      "_source": {
         |        "enabled": true
         |      },
         |
         |      "properties": {
         |        ${attributes}
         |      }
         |  }
         |}""".stripMargin
      }
      .richFormat(schemaHandler.activeEnvVars(), tse)
  }

  def mergedMetadata(domainMetadata: Option[Metadata]): Metadata = {
    domainMetadata
      .getOrElse(Metadata())
      .merge(this.metadata.getOrElse(Metadata()))

  }

  def containsArrayOfRecords(): Boolean = attributes.exists(_.containsArrayOfRecords())

  private def dotRow(
    attr: Attribute,
    isPK: Boolean,
    isFK: Boolean,
    includeAllAttrs: Boolean
  ): Option[String] = {
    val col = attr.default match {
      case None    => s"""${attr.getFinalName()}:${attr.`type`}"""
      case Some(x) => s"""${attr.getFinalName()}:${attr.`type`} = $x"""
    }
    (isPK, isFK, includeAllAttrs) match {
      case (true, true, _) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"><B><I> $col </I></B></td></tr>""")
      case (true, false, _) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"><B> $col </B></td></tr>""")
      case (false, true, _) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"><I> $col </I></td></tr>""")
      case (false, false, true) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"> $col </td></tr>""")
      case (false, false, false) => None
    }
  }

  private def dotRelation(attr: Attribute, domain: String): Option[String] = {
    val tableLabel = s"${domain}_$name"
    attr.foreignKey match {
      case None => None
      case Some(ref) =>
        val tab = ref.split('.')
        val (refDomain, refSchema, refAttr) = tab.length match {
          case 3 =>
            (tab(0), tab(1), if (tab(2).isEmpty) "0" else tab(2)) // ref to domain.table.column
          case 2 =>
            (domain, tab(0), if (tab(1).isEmpty) "0" else tab(1)) // ref to table.column
          case 1 => (domain, tab(0), "0") // ref to table
          case _ =>
            throw new Exception(
              s"Invalid number of parts in relation $ref in domain $domain and table $name"
            )
        }
        Some(s"$tableLabel:${attr.name} -> ${refDomain}_$refSchema:$refAttr")
    }
  }

  def fkComponents(attr: Attribute, domain: String): Option[(Attribute, String, String, String)] = { // (attr, refDomain, refSchema, refAttr)
    attr.foreignKey match {
      case None => None
      case Some(ref) =>
        val tab = ref.split('.')
        val (refDomain, refSchema, refAttr) = tab.length match {
          case 3 =>
            (
              tab(0),
              tab(1),
              if (tab(2).isEmpty) attr.getFinalName() else tab(2)
            ) // ref to domain.table.column
          case 2 =>
            (domain, tab(0), if (tab(1).isEmpty) this.finalName else tab(1)) // ref to table.column
          case 1 => (domain, tab(0), attr.getFinalName()) // ref to table
          case _ =>
            throw new Exception(
              s"Invalid number of parts in relation $ref in domain $domain and table $name"
            )
        }
        Some((attr, refDomain, refSchema, refAttr))
    }
  }

  @JsonIgnore
  def hasACL(): Boolean =
    acl.nonEmpty

  def relatedTables(): List[String] = {
    val fkTables = attributes.flatMap(_.foreignKey).map { fk =>
      val tab = fk.split('.')
      tab.length match {
        case 3 => tab(1) // reference to domain.table.column
        case 2 => tab(0) // reference to table.column
        case 1 => tab(0) // reference to table
      }
    }
    if (fkTables.nonEmpty)
      fkTables :+ finalName
    else
      fkTables
  }

  def asDot(domain: String, includeAllAttrs: Boolean, fkTables: Set[String]): String = {
    val isFKTable = fkTables.contains(finalName.toLowerCase)
    if (isFKTable) {
      val tableLabel = s"${domain}_$finalName"
      val header =
        s"""<tr><td port="0" bgcolor="white"><B><FONT color="black"> $finalName </FONT></B></td></tr>\n"""
      val rows =
        attributes.flatMap { attr =>
          val isPK = primaryKey.contains(attr.getFinalName())
          val isFK = attr.foreignKey.isDefined
          dotRow(attr, isPK, isFK, includeAllAttrs)
        } mkString "\n"

      val relations = attributes
        .flatMap { attr => dotRelation(attr, domain) }
        .mkString("\n")

      s"""
         |$tableLabel [label=<
         |<table border="0" cellborder="1" cellspacing="0">
         |""".stripMargin +
      header +
      rows +
      """
          |</table>>];
          |
          |""".stripMargin +
      relations
    } else {
      ""
    }
  }

  /** @param table
    *   table to add field to
    * @param sourceTableFilter
    *   filter applied after transformation and before field removal
    * @return
    *   query
    */
  def buildSqlSelect(
    table: String,
    sourceTableFilter: Option[String],
    columnSuffixOpt: Option[String] = None
  ): String = {
    val (scriptAttributes, transformAttributes) =
      scriptAndTransformAttributes().partition(_.script.nonEmpty)

    val simpleAttributes = exceptIgnoreScriptAndTransformAttributes()

    val sqlScripts: List[String] = scriptAttributes.map { scriptField =>
      val script = scriptField.script.getOrElse(throw new Exception("Should never happen"))
      s"$script AS `${scriptField.getFinalName()}`"
    }
    val sqlTransforms: List[String] = transformAttributes.map { transformField =>
      val transform = transformField.transform.getOrElse(throw new Exception("Should never happen"))
      s"$transform AS `${transformField.getFinalName()}`"
    }

    val sqlSimple = simpleAttributes.map { field =>
      s"`${field.getFinalName()}`"
    }

    val sqlIgnored = ignoredAttributes().map { field =>
      s"`${field.getFinalName()}`"
    }

    val allAttributes = (sqlSimple ++ sqlScripts ++ sqlTransforms ++ sqlIgnored).mkString(", ")
    val allFinalAttributes = columnSuffixOpt match {
      case Some(columnSuffix) =>
        (simpleAttributes.map(_.getFinalName()) ++ scriptAttributes.map(
          _.getFinalName()
        ) ++ transformAttributes.map(_.getFinalName()))
          .map(f => s"`$f` AS `$f$columnSuffix`")
          .mkString(", ")
      case None =>
        val finalSqlExcept = {
          if (ignoredAttributes().isEmpty) {
            ""
          } else {
            sqlIgnored.mkString("EXCEPT(", ",", ")")
          }
        }
        s"* $finalSqlExcept"
    }
    val sourceTableFilterSQL = sourceTableFilter match {
      case Some(filter) => s"WHERE $filter"
      case None         => ""

    }
    s"""
       |SELECT $allFinalAttributes
       |  FROM (
       |    SELECT $allAttributes
       |    FROM $table
       |  )
       |  $sourceTableFilterSQL
       |""".stripMargin
  }

  def buildSqlMerge(
    sourceTable: String,
    targetTable: String,
    mergeOptionsOpt: Option[MergeOptions],
    sourceTableFilter: Option[String],
    targetTableFilters: List[String],
    updateTargetFilters: List[String],
    partitionOverwrite: Boolean
  ): String = {
    val (scriptAttributes, transformAttributes) =
      scriptAndTransformAttributes().partition(_.script.nonEmpty)

    val simpleAttributes = exceptIgnoreScriptAndTransformAttributes()
    val allOutputAttributes = simpleAttributes ++ transformAttributes ++ scriptAttributes

    val targetTableFilterSQL = targetTableFilters match {
      case Nil => ""
      case _   => targetTableFilters.mkString(" AND ")
    }

    if (partitionOverwrite) { // similar to dynamic mode in spark
      val (targetColumns, sourceColumns) =
        allOutputAttributes
          .map(f => s"`${f.getFinalName()}`" -> s"$SL_INTERNAL_TABLE.`${f.getFinalName()}`")
          .unzip
      val notMatchedInsertColumnsSql = targetColumns.mkString("(", ",", ")")
      val notMatchedInsertValuesSql = sourceColumns.mkString("(", ",", ")")
      val notMatchedInsertSql = s"""$notMatchedInsertColumnsSql VALUES $notMatchedInsertValuesSql"""
      val updateTargetFiltersSQL = updateTargetFilters match {
        case Nil =>
          throw new RuntimeException("No filter applied for partition overwrite. Should not happen")
        case _ => updateTargetFilters.mkString(" AND ")
      }
      val inputData =
        if (merge.map(_.key.isEmpty).getOrElse(true))
          buildSqlSelect(
            sourceTable,
            sourceTableFilter
          ) // partition overwrite without deduplication
        else
          buildSqlMerge(
            sourceTable,
            targetTable,
            mergeOptionsOpt,
            sourceTableFilter,
            targetTableFilters,
            updateTargetFilters,
            false
          )
      val joinAdditionalClauseSQL =
        if (updateTargetFiltersSQL.trim.isEmpty) "" else f"AND $updateTargetFiltersSQL"
      s"""MERGE INTO $targetTable $SL_TARGET_TABLE USING ($inputData) AS $SL_INTERNAL_TABLE ON FALSE
         |WHEN NOT MATCHED BY SOURCE $joinAdditionalClauseSQL THEN DELETE
         |WHEN NOT MATCHED $joinAdditionalClauseSQL THEN INSERT $notMatchedInsertSql
         |""".stripMargin
    } else {
      val inputData = buildSqlSelect(sourceTable, sourceTableFilter)
      val allAttributesSQL = allOutputAttributes
        .map { attribute =>
          s"`${attribute.getFinalName()}`"
        }
        .mkString(",")
      val dataSourceColumnName = "SL_DATASOURCE_INFORMATION"
      // According to the usage above of join clause between target and source table, we assume key not to be null.
      val partitionKeys =
        mergeOptionsOpt
          .map(_.key.map(key => s"`$key`").mkString(","))
          .getOrElse(throw new RuntimeException("Should not happen"))
      val rowSelectionSQL = mergeOptionsOpt.flatMap(_.timestamp) match {
        case Some(mergeTimestampCol) =>
          s"QUALIFY row_number() OVER (PARTITION BY $partitionKeys ORDER BY `$mergeTimestampCol` DESC) = 1"
        case _ =>
          // use dense_rank instead of row_number in order to have the same behavior as in spark ingestion
          s"QUALIFY DENSE_RANK() OVER (PARTITION BY $partitionKeys ORDER BY CASE $dataSourceColumnName WHEN '$SL_INTERNAL_TABLE' THEN 2 ELSE 1 END DESC) = 1"
      }

      val whereClauseSQL = if (targetTableFilterSQL.isEmpty) "" else s"WHERE $targetTableFilterSQL"

      s"""SELECT * EXCEPT($dataSourceColumnName) FROM(
         |  SELECT $allAttributesSQL, '$SL_INTERNAL_TABLE' as `$dataSourceColumnName`  FROM ($inputData)
         |  UNION ALL
         |  SELECT $allAttributesSQL, '$SL_TARGET_TABLE' as `$dataSourceColumnName` FROM $targetTable $SL_TARGET_TABLE
         |  $whereClauseSQL
         |)
         |$rowSelectionSQL
         |""".stripMargin
    }
  }

  /** @param fallbackSchema
    *   complete missing information with this schema
    * @param domainMetadata
    *   metadata to compare with. Useful to keep only things that are different.
    * @return
    *   merged schema
    */
  def mergeWith(
    fallbackSchema: Schema,
    domainMetadata: Option[Metadata] = None,
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit
    schemaHandler: SchemaHandler
  ) = {
    this.copy(
      rename = this.rename.orElse(fallbackSchema.rename),
      comment = this.comment.orElse(fallbackSchema.comment),
      metadata = Metadata
        .mergeAll(Nil ++ domainMetadata ++ fallbackSchema.metadata ++ this.metadata)
        .`keepIfDifferent`(domainMetadata.getOrElse(Metadata()))
        .asOption(),
      merge = this.merge.orElse(fallbackSchema.merge),
      presql = if (this.presql.isEmpty) fallbackSchema.presql else this.presql,
      postsql = if (this.postsql.isEmpty) fallbackSchema.postsql else this.postsql,
      tags = if (this.tags.isEmpty) fallbackSchema.tags else this.tags,
      rls = if (this.rls.isEmpty) fallbackSchema.rls else this.rls,
      expectations =
        if (this.expectations.isEmpty) fallbackSchema.expectations else this.expectations,
      acl = if (this.acl.isEmpty) fallbackSchema.acl else this.acl,
      sample = this.sample.orElse(fallbackSchema.sample),
      filter = this.filter.orElse(fallbackSchema.filter),
      attributes = Attribute.mergeAll(
        this.attributes,
        fallbackSchema.attributes,
        attributeMergeStrategy
      )
    )
  }

}

object Schema {

  val SL_INTERNAL_TABLE = "SL_INTERNAL_TABLE"

  val SL_TARGET_TABLE = "SL_TARGET_TABLE"

  def mapping(
    domainName: String,
    schemaName: String,
    obj: StructField,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): String = {
    fromSparkSchema(schemaName, obj).esMapping(
      None,
      domainName,
      schemaHandler
    )
  }
  def fromSparkSchema(
    schemaName: String,
    obj: StructField
  )(implicit settings: Settings): Schema = {
    def buildAttributeTree(obj: StructField): Attribute = {
      obj.dataType match {
        case StringType | LongType | IntegerType | ShortType | DoubleType | BooleanType | ByteType |
            DateType | TimestampType =>
          Attribute(
            obj.name,
            obj.dataType.typeName,
            required = !obj.nullable,
            comment = obj.getComment()
          )
        case _: DecimalType =>
          Attribute(obj.name, "decimal", required = !obj.nullable, comment = obj.getComment())
        case ArrayType(eltType, containsNull) => buildAttributeTree(obj.copy(dataType = eltType))
        case x: StructType =>
          Attribute(
            obj.name,
            "struct",
            required = !obj.nullable,
            attributes = x.fields.map(buildAttributeTree).toList,
            comment = obj.getComment()
          )
        case _ => throw new Exception(s"Unsupported Date type ${obj.dataType} for object $obj ")
      }
    }

    Schema(
      schemaName,
      Pattern.compile("ignore"),
      buildAttributeTree(obj).attributes,
      None,
      None,
      None,
      Nil,
      Nil
    )
  }

  def fromTaskDesc(taskDesc: AutoTaskDesc): Schema = {
    val attributes: List[Attribute] = taskDesc.attributesDesc.map { ad =>
      Attribute(name = ad.name, accessPolicy = ad.accessPolicy, comment = Some(ad.comment))
    }
    Schema(
      name = taskDesc.name,
      pattern = Pattern.compile(taskDesc.name),
      attributes = attributes,
      None,
      None,
      taskDesc.comment,
      tags = taskDesc.tags
    )
  }

  def compare(existing: Schema, incoming: Schema): Try[SchemaDiff] = {
    Try {
      if (!existing.isFlat() || !incoming.isFlat())
        throw new Exception("Only flat schemas are supported")

      val patternDiff: ListDiff[String] =
        if (existing.pattern.toString != incoming.pattern.toString)
          ListDiff(
            "pattern",
            Nil,
            Nil,
            List((existing.pattern.toString, incoming.pattern.toString))
          )
        else {
          ListDiff(
            "pattern",
            Nil,
            Nil,
            Nil
          )
        }
      val attributesDiff =
        AnyRefDiff.diffListNamed("attributes", existing.attributes, incoming.attributes)

      val metadataDiff: ListDiff[Named] =
        AnyRefDiff.diffOptionAnyRef("metadata", existing.metadata, incoming.metadata)

      val mergeDiff: ListDiff[Named] =
        AnyRefDiff.diffOptionAnyRef("merge", existing.merge, incoming.merge)

      val commentDiff: ListDiff[String] =
        AnyRefDiff.diffOptionString("comment", existing.comment, incoming.comment)

      val presqlDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("presql", existing.presql.toSet, incoming.presql.toSet)

      val postsqlDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("postsql", existing.postsql.toSet, incoming.postsql.toSet)

      val tagsDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("tags", existing.tags, incoming.tags)

      val rlsDiff: ListDiff[Named] = AnyRefDiff.diffListNamed("rls", existing.rls, incoming.rls)

      val expectationsDiff: ListDiff[Named] =
        AnyRefDiff.diffMap("expectations", existing.expectations, incoming.expectations)

      val primaryKeyDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("primaryKey", existing.primaryKey.toSet, incoming.primaryKey.toSet)

      val aclDiff: ListDiff[Named] = AnyRefDiff.diffListNamed("acl", existing.acl, incoming.acl)

      val renameDiff: ListDiff[String] =
        AnyRefDiff.diffOptionString("rename", existing.rename, incoming.rename)

      val sampleDiff: ListDiff[String] =
        AnyRefDiff.diffOptionString("sample", existing.sample, incoming.sample)

      val filter: ListDiff[String] =
        AnyRefDiff.diffOptionString("filter", existing.filter, incoming.filter)

      SchemaDiff(
        existing.name,
        attributesDiff,
        patternDiff,
        metadataDiff,
        mergeDiff,
        commentDiff,
        presqlDiff,
        postsqlDiff,
        tagsDiff,
        rlsDiff,
        expectationsDiff,
        primaryKeyDiff,
        aclDiff,
        renameDiff,
        sampleDiff,
        filter
      )
    }
  }
}

case class SchemaRefs(tables: List[Schema])
case class SchemaRef(table: Schema)
