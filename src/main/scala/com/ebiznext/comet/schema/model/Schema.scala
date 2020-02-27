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

import java.util.regex.Pattern

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.TextSubstitutionEngine
import com.google.cloud.bigquery.{Field, LegacySQLTypeName}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * How dataset are merge
  *
  * @param key    list of attributes to join existing with incoming dataset. Use renamed columns here.
  * @param delete Optional valid sql condition on the incoming dataset. Use renamed column here.
  * @param timestamp Timestamp column used to identify last version, if not specified currently ingested row is considered the last
  */
case class MergeOptions(
  key: List[String],
  delete: Option[String] = None,
  timestamp: Option[String] = None
)

/**
  * Dataset Schema
  *
  * @param name       : Schema name, must be unique in the domain. Will become the hive table name
  * @param pattern    : filename pattern to which this schema must be applied
  * @param attributes : datasets columns
  * @param metadata   : Dataset metadata
  * @param comment    : free text
  * @param presql     :  SQL code executed before the file is ingested
  * @param postsql    : SQL code executed right after the file has been ingested
  */
case class Schema(
  name: String,
  pattern: Pattern,
  attributes: List[Attribute],
  metadata: Option[Metadata],
  merge: Option[MergeOptions],
  comment: Option[String],
  presql: Option[List[String]],
  postsql: Option[List[String]],
  tags: Option[Set[String]] = None
) {

  /**
    * @return Are the parittions columns defined in the metadata valid column names
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

  /**
    * This Schema as a Spark Catalyst Schema
    *
    * @return Spark Catalyst Schema
    */
  def sparkType()(implicit settings: Settings): StructType = {
    val fields = attributes.map { attr =>
      StructField(attr.name, attr.sparkType(), !attr.required)
    }
    StructType(fields)
  }

  import com.google.cloud.bigquery.{Schema => BQSchema}

  def bqSchema()(implicit settings: Settings): BQSchema = {
    def convert(sparkType: DataType): LegacySQLTypeName = {

      val BQ_NUMERIC_PRECISION = 38
      val BQ_NUMERIC_SCALE = 9
      lazy val NUMERIC_SPARK_TYPE =
        DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE)

      sparkType match {
        case BooleanType                                     => LegacySQLTypeName.BOOLEAN
        case ByteType | LongType | IntegerType               => LegacySQLTypeName.INTEGER
        case DoubleType | FloatType                          => LegacySQLTypeName.FLOAT
        case StringType                                      => LegacySQLTypeName.STRING
        case BinaryType                                      => LegacySQLTypeName.BYTES
        case DateType                                        => LegacySQLTypeName.DATE
        case TimestampType                                   => LegacySQLTypeName.TIMESTAMP
        case DecimalType.SYSTEM_DEFAULT | NUMERIC_SPARK_TYPE => LegacySQLTypeName.NUMERIC
        case _                                               => throw new IllegalArgumentException(s"Unsupported type:$sparkType")
      }
    }
    val fields = attributes map { attribute =>
      Field
        .newBuilder(attribute.rename.getOrElse(attribute.name), convert(attribute.sparkType()))
        .setMode(if (attribute.required) Field.Mode.REQUIRED else Field.Mode.NULLABLE)
        .setDescription(attribute.comment.getOrElse(""))
        .build()
    }
    BQSchema.of(fields: _*)
  }

  /**
    * return the list of renamed attributes
    *
    * @return list of tuples (oldname, newname)
    */
  def renamedAttributes(): List[(String, String)] = {
    attributes.filter(attr => attr.name != attr.getFinalName()).map { attr =>
      (attr.name, attr.getFinalName())
    }
  }

  /**
    * Check attribute definition correctness :
    *   - schema name should be a valid table identifier
    *   - attribute name should be a valid Hive column identifier
    *   - attribute name can occur only once in the schema
    *
    * @return error list or true
    */
  def checkValidity(
    domainMetaData: Option[Metadata]
  )(implicit settings: Settings): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val tableNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,256}")
    if (!tableNamePattern.matcher(name).matches())
      errorList += s"Schema with name $name should respect the pattern ${tableNamePattern.pattern()}"

    attributes.foreach { attribute =>
      for (errors <- attribute.checkValidity().left) {
        errorList ++= errors
      }
    }

    val duplicateErrorMessage =
      "%s is defined %d times. An attribute can only be defined once."
    for (errors <- duplicates(attributes.map(_.name), duplicateErrorMessage).left) {
      errorList ++= errors
    }
    val format = this.mergedMetadata(domainMetaData).format
    format match {
      case Some(Format.POSITION) =>
        val attrsAsArray = attributes.toArray
        for (i <- 0 until attrsAsArray.length - 1) {
          val pos1 = attrsAsArray(i).position
          val pos2 = attrsAsArray(i + 1).position
          (pos1, pos2) match {
            case (Some(pos1), Some(pos2)) =>
              if (pos1.last >= pos2.first) {
                errorList += s"Positions should be ordered : ${pos1.last} > ${pos2.first}"
              }
            case (_, _) =>
              errorList += s"All attributes should have their position defined"

          }
        }
      case _ =>
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def discreteAttrs()(implicit settings: Settings): List[Attribute] =
    attributes.filter(_.getMetricType() == MetricType.DISCRETE)

  def continuousAttrs()(implicit settings: Settings): List[Attribute] =
    attributes.filter(_.getMetricType() == MetricType.CONTINUOUS)

  def mapping(template: Option[String], domainName: String)(implicit settings: Settings): String = {
    val attrs = attributes.map(_.mapping()).mkString(",")
    val properties =
      s"""
         |"properties": {
         |$attrs
         |}""".stripMargin

    val tse = TextSubstitutionEngine("PROPERTIES" -> properties, "ATTRIBUTES" -> attrs)

    tse.apply(template.getOrElse {
      s"""
         |{
         |  "index_patterns": ["${domainName}_$name", "${domainName}_$name-*"],
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
         |£ATTRIBUTES£
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

  def mapping(domainName: String, schemaName: String, obj: StructField)(
    implicit settings: Settings
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
    ).mapping(None, domainName)
  }
}
