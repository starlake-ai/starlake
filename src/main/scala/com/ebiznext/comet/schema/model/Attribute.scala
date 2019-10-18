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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * A field in the schema. For struct fields, the field "attributes" contains all sub attributes
  *
  * @param name       : Attribute name as defined in the source dataset
  * @param `type`     : semantic type of the attribute
  * @param array      : Is it an array ?
  * @param required   : Should this attribute always be present in the source
  * @param privacy    : Should this attribute be applied a privacy transformaiton at ingestion time
  * @param comment    : free text for attribute description
  * @param rename     : If present, the attribute is renamed with this name
  * @param metricType       : If present, what kind of stat should be computed for this field
  * @param attributes : List of sub-attributes
  * @param position   : Valid only where file format is POSITION
  */
case class Attribute(
  name: String,
  `type`: String = "string",
  array: Option[Boolean] = None,
  required: Boolean = true,
  privacy: Option[PrivacyLevel] = None,
  comment: Option[String] = None,
  rename: Option[String] = None,
  metricType: Option[MetricType] = None,
  attributes: Option[List[Attribute]] = None,
  position: Option[Position] = None,
  default: Option[String] = None,
  tags: Option[Set[String]] = None
) extends LazyLogging {

  /**
    * Check attribute validity
    * An attribute is valid if :
    *     - Its name is a valid identifier
    *     - its type is defined
    *     - When a privacy function is defined its primitive type is a string
    *
    * @return true if attribute is valid
    */
  def checkValidity(): Either[List[String], Boolean] = {
    import com.ebiznext.comet.config.Settings.schemaHandler
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    if (`type` == null)
      errorList += s"$this : unspecified type"

    val colNamePattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]{1,767}")
    //if (!colNamePattern.matcher(name).matches())
    //  errorList += s"attribute with name $name should respect the pattern ${colNamePattern.pattern()}"

    if (!rename.forall(colNamePattern.matcher(_).matches()))
      errorList += s"renamed attribute with renamed name '$rename' should respect the pattern ${colNamePattern.pattern()}"

    val tpe = schemaHandler.types.find(_.name == `type`)
    val primitiveType = tpe.map(_.primitiveType)

    primitiveType match {
      case Some(tpe) =>
        if (tpe != PrimitiveType.string && getPrivacy() != PrivacyLevel.None)
          errorList += s"Attribute $this : string is the only supported primitive type for an attribute when privacy is requested"
        if (tpe == PrimitiveType.struct && attributes.isEmpty)
          errorList += s"Attribute $this : Struct types have at least one attribute."
        if (tpe != PrimitiveType.struct && attributes.isDefined)
          errorList += s"Attribute $this : Simple attributes cannot have sub-attributes"
      case None if attributes.isEmpty => errorList += s"Invalid Type ${`type`}"
      case _                          => // good boy
    }

    default.foreach { default =>
      if (required)
        errorList += s"attribute with name $name: default value valid for optional fields only"
      primitiveType.foreach { primitiveType =>
        if (primitiveType == PrimitiveType.struct)
          errorList += s"attribute with name $name: default value not valid for struct type fields"
        tpe.foreach {
          _.sparkValue(default) match {
            case Success(_) =>
            case Failure(e) =>
              errorList += s"attribute with name $name: Invalid default value for tha attribute type ${e.getMessage()}"
          }
        }
      }
      array.foreach { isArray =>
        if (isArray)
          errorList += s"attribute with name $name: default value not valid for array fields"
      }
    }

    attributes.collect {
      case list if list.isEmpty =>
        errorList += s"Attribute $this : when present, attributes list cannot be empty."
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  lazy val tpe: Option[Type] = {
    import com.ebiznext.comet.config.Settings.schemaHandler
    schemaHandler.types
      .find(_.name == `type`)
  }

  /**
    *
    * Spark Type if this attribute is a primitive type of array of primitive type
    *
    * @return Primitive type if attribute is a leaf node or array of primitive type, None otherwise
    */
  def primitiveSparkType(): DataType = {
    tpe
      .map(_.primitiveType)
      .map { tpe =>
        if (isArray())
          ArrayType(tpe.sparkType, !required)
        else
          tpe.sparkType
      }
      .getOrElse(PrimitiveType.struct.sparkType)
  }

  /**
    * Go get recursively the Spark tree type of this object
    *
    * @return Spark type of this attribute
    */
  def sparkType(): DataType = {
    val tpe = primitiveSparkType()
    tpe match {
      case _: StructType =>
        attributes.map { attrs =>
          val fields = attrs.map { attr =>
            StructField(attr.name, attr.sparkType(), !attr.required)
          }
          if (isArray())
            ArrayType(StructType(fields))
          else
            StructType(fields)
        } getOrElse (throw new Exception("Should never happen: empty list of attributes"))

      case simpleType => simpleType
    }
  }

  def mapping(): String = {
    attributes match {
      case Some(attrs) =>
        s"""
           |"$name": {
           |  "properties" : {
           |  ${attrs.map(_.mapping()).mkString(",")}
           |  }
           |}""".stripMargin

      case None =>
        tpe.map { tpe =>
          val typeMapping = tpe.getIndexMapping().toString
          tpe.primitiveType match {
            case PrimitiveType.date =>
              //  "format" : "${tpe.pattern}"
              s"""
                 |"$name": {
                 |  "type": "$typeMapping"
                 |}""".stripMargin
            case PrimitiveType.timestamp =>
              val format = tpe.pattern match {
                case "epoch_milli"                                         => Some("epoch_millis")
                case "epoch_second"                                        => Some("epoch_second")
                case x if PrimitiveType.dateFormatters.keys.exists(_ == x) => None
                case y                                                     => Some(y)
              }
              format match {
                case Some(fmt) =>
                  //"format" : "$fmt"
                  s"""
                     |"$name": {
                     |"type": "$typeMapping"
                     |}""".stripMargin
                case None => // Not Supported date format for ES TODO : needs to be implemented
                  s"""
                     |"$name": {
                     |"type": "keyword"
                     |}""".stripMargin
              }

            case _ =>
              s"""
                 |"$name": {
                 |  "type": "$typeMapping"
                 |}""".stripMargin
          }
        } getOrElse (throw new Exception("Cannot map unknown type"))
    }
  }

  /**
    * @return renamed column if defined, source name otherwise
    */
  @JsonIgnore
  def getFinalName(): String = rename.getOrElse(name)

  def getPrivacy(): PrivacyLevel = this.privacy.getOrElse(PrivacyLevel.None)

  def isArray(): Boolean = this.array.getOrElse(false)

  def isRequired(): Boolean = Option(this.required).getOrElse(false)

  @JsonIgnore
  def getMetricType(): MetricType = {
    import com.ebiznext.comet.utils.DataTypeEx._
    val sparkType = tpe.map(_.primitiveType.sparkType)
    logger.info(s"Attribute Metric ==> $name, $metricType, $sparkType")
    (sparkType, metricType) match {
      case (Some(sparkType), Some(MetricType.DISCRETE)) =>
        if (sparkType.isOfValidDiscreteType()) {
          MetricType.DISCRETE
        } else
          MetricType.NONE
      case (Some(sparkType), Some(MetricType.CONTINUOUS)) =>
        if (sparkType.isOfValidContinuousType()) {
          MetricType.CONTINUOUS
        } else
          MetricType.NONE
      case (_, _) =>
        MetricType.NONE
    }

  }
}
