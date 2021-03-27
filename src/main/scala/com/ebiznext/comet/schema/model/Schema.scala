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

package com.ebiznext.comet.schema.model

import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.ebiznext.comet.utils.TextSubstitutionEngine
import com.ebiznext.comet.utils.conversion.BigQueryUtils
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types._

import java.util.regex.Pattern
import scala.collection.mutable
import com.google.cloud.bigquery.{Schema => BQSchema}

/** How dataset are merged
  *
  * @param key    list of attributes to join existing with incoming dataset. Use renamed columns here.
  * @param delete Optional valid sql condition on the incoming dataset. Use renamed column here.
  * @param timestamp Timestamp column used to identify last version, if not specified currently ingested row is considered the last
  */
case class MergeOptions(
  key: List[String],
  delete: Option[String] = None,
  timestamp: Option[String] = None,
  queryFilter: Option[String] = None
)

/** Dataset Schema
  *
  * @param name       : Schema name, must be unique among all the schemas belonging to the same domain.
  *                     Will become the hive table name On Premise or BigQuery Table name on GCP.
  * @param pattern    : filename pattern to which this schema must be applied.
  *                     This instructs the framework to use this schema to parse any file with a filename that match this pattern.
  * @param attributes : Attributes parsing rules.
  *                     See :ref:`attribute_concept`
  * @param metadata   : Dataset metadata
  *                     See :ref:`metadata_concept`
  * @param comment    : free text
  * @param presql     : Reserved for future use.
  * @param postsql    : We use this attribute to execute sql queries before writing the final dataFrame after ingestion
  * @param tags       : Set of string to attach to this Schema
  * @param rls        : Experimental. Row level security to this to this schema.
  *                     See :ref:`rowlevelsecurity_concept`
  */
case class Schema(
  name: String,
  pattern: Pattern,
  attributes: List[Attribute],
  metadata: Option[Metadata],
  merge: Option[MergeOptions],
  comment: Option[String],
  presql: Option[List[String]],
  postsql: Option[List[String]] = None,
  tags: Option[Set[String]] = None,
  rls: Option[List[RowLevelSecurity]] = None,
  assertions: Option[Map[String, String]] = None
) {

  @JsonIgnore
  lazy val attributesWithoutScript: List[Attribute] = attributes.filter(_.script.isEmpty)

  /** @return Are the parittions columns defined in the metadata valid column names
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

  /** This Schema as a Spark Catalyst Schema
    *
    * @return Spark Catalyst Schema
    */
  def sparkType(schemaHandler: SchemaHandler): StructType = {
    val fields = attributes.map { attr =>
      StructField(attr.name, attr.sparkType(schemaHandler), !attr.required)
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  /** This Schema as a Spark Catalyst Schema, with renamed attributes
    *
    * @return Spark Catalyst Schema
    */
  def sparkTypeWithRenamedFields(schemaHandler: SchemaHandler): StructType =
    sparkSchemaWithCondition(schemaHandler, _ => true)

  /** This Schema as a Spark Catalyst Schema, without scripted fields
    *
    * @return Spark Catalyst Schema
    */
  def sparkSchemaWithoutScriptedFields(schemaHandler: SchemaHandler): StructType =
    sparkSchemaWithCondition(schemaHandler, _.script.isEmpty)

  def attributesWithoutScriptedFields(): List[Attribute] = attributes filter (_.script.isEmpty)

  private def sparkSchemaWithCondition(
    schemaHandler: SchemaHandler,
    p: Attribute => Boolean
  ): StructType = {
    val fields = attributes filter p map { attr =>
      StructField(attr.rename.getOrElse(attr.name), attr.sparkType(schemaHandler), !attr.required)
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  def bigQuerySchema(schemaHandler: SchemaHandler): BQSchema = {
    BigQueryUtils.bqSchema(sparkTypeWithRenamedFields(schemaHandler))
  }

  /** return the list of renamed attributes
    *
    * @return list of tuples (oldname, newname)
    */
  def renamedAttributes(): List[(String, String)] = {
    attributes.filter(attr => attr.name != attr.getFinalName()).map { attr =>
      (attr.name, attr.getFinalName())
    }
  }

  /** Check attribute definition correctness :
    *   - schema name should be a valid table identifier
    *   - attribute name should be a valid Hive column identifier
    *   - attribute name can occur only once in the schema
    *
    * @return error list or true
    */
  def checkValidity(
    domainMetaData: Option[Metadata],
    schemaHandler: SchemaHandler
  ): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val tableNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,256}")
    if (!tableNamePattern.matcher(name).matches())
      errorList += s"Schema with name $name should respect the pattern ${tableNamePattern.pattern()}"

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
        s"""Scripted fields can only appear at the end of the schema. Found scripted field at position $firstScriptedFiedlIndex and non scripted field at position $lastNonScriptedFiedlIndex""".stripMargin
    }

    val duplicateErrorMessage =
      "%s is defined %d times. An attribute can only be defined once."
    for (errors <- duplicates(attributes.map(_.name), duplicateErrorMessage).left) {
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

  def mapping(
    template: Option[String],
    domainName: String,
    schemaHandler: SchemaHandler
  ): String = {
    val attrs = attributes.map(_.mapping(schemaHandler)).mkString(",")
    val properties =
      s"""
         |"properties": {
         |$attrs
         |}""".stripMargin

    val tse = TextSubstitutionEngine(
      "PROPERTIES" -> properties,
      "ATTRIBUTES" -> attrs,
      "DOMAIN"     -> domainName.toLowerCase,
      "SCHEMA"     -> name.toLowerCase
    )

    tse.apply(template.getOrElse {
      s"""
         |{
         |  "index_patterns": ["__DOMAIN__.__SCHEMA__", "__DOMAIN__.__SCHEMA__-*"],
         |  "settings": {
         |    "number_of_shards": "1",
         |    "number_of_replicas": "0"
         |  },
         |  "mappings": {
         |    "_doc": {
         |      "_source": {
         |        "enabled": true
         |      },
         |
         |"properties": {
         |__ATTRIBUTES__
         |}
         |    }
         |  }
         |}""".stripMargin
    })
  }

  def mergedMetadata(domainMetadata: Option[Metadata]): Metadata = {
    domainMetadata
      .getOrElse(Metadata())
      .`import`(this.metadata.getOrElse(Metadata()))

  }
}

object Schema {

  def mapping(
    domainName: String,
    schemaName: String,
    obj: StructField,
    schemaHandler: SchemaHandler
  ): String = {
    def buildAttributeTree(obj: StructField): Attribute = {
      obj.dataType match {
        case StringType | LongType | IntegerType | ShortType | DoubleType | BooleanType | ByteType |
            DateType | TimestampType =>
          Attribute(obj.name, obj.dataType.typeName, required = !obj.nullable)
        case d: DecimalType =>
          Attribute(obj.name, "decimal", required = !obj.nullable)
        case ArrayType(eltType, containsNull) => buildAttributeTree(obj.copy(dataType = eltType))
        case x: StructType =>
          new Attribute(
            obj.name,
            "struct",
            required = !obj.nullable,
            attributes = Some(x.fields.map(buildAttributeTree).toList)
          )
        case _ => throw new Exception(s"Unsupported Date type ${obj.dataType} for object $obj ")
      }
    }

    Schema(
      schemaName,
      Pattern.compile("ignore"),
      buildAttributeTree(obj).attributes.getOrElse(Nil),
      None,
      None,
      None,
      None,
      None
    ).mapping(None, domainName, schemaHandler)
  }
}
