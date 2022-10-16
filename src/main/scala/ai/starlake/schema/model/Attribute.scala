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

import java.util.regex.Pattern
import ai.starlake.schema.handlers.SchemaHandler
import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import ai.starlake.utils.DataTypeEx._
import ai.starlake.utils.Utils

/** A field in the schema. For struct fields, the field "attributes" contains all sub attributes
  *
  * @param name
  *   : Attribute name as defined in the source dataset and as received in the file
  * @param `type`
  *   : semantic type of the attribute.
  * @param array
  *   : Is it an array ?
  * @param required
  *   : Should this attribute always be present in the source
  * @param privacy
  *   : Should this attribute be applied a privacy transformation at ingestion time
  * @param comment
  *   : free text for attribute description
  * @param rename
  *   : If present, the attribute is renamed with this name
  * @param metricType
  *   : If present, what kind of stat should be computed for this field
  * @param attributes
  *   : List of sub-attributes (valid for JSON and XML files only)
  * @param position
  *   : Valid only when file format is POSITION
  * @param default
  *   : Default value for this attribute when it is not present.
  * @param tags
  *   : Tags associated with this attribute
  * @param trim
  *   : Should we trim the attribute value ?
  * @param script
  *   : Scripted field : SQL request on renamed column
  */
case class Attribute(
  name: String,
  `type`: String = "string",
  array: Option[Boolean] = None,
  required: Boolean = true,
  privacy: PrivacyLevel = PrivacyLevel.None,
  comment: Option[String] = None,
  rename: Option[String] = None,
  metricType: Option[MetricType] = None,
  attributes: Option[List[Attribute]] = None,
  position: Option[Position] = None,
  default: Option[String] = None,
  tags: Option[Set[String]] = None,
  trim: Option[Trim] = None,
  script: Option[String] = None,
  foreignKey: Option[String] = None, // [domain.]table.attribute
  ignore: Option[Boolean] = None,
  accessPolicy: Option[String] = None
) extends LazyLogging {

  /** Bring properties from another attribue. Used in XSD handling Exclude some properties: name,
    * type, required, array, attributes, position, script
    * @param imported
    * @return
    *   merged attribute
    */
  private def importAttributeProperties(imported: Attribute): Attribute = {
    this.copy(
      privacy = imported.privacy,
      comment = imported.comment.orElse(this.comment),
      rename = imported.rename.orElse(this.rename),
      metricType = imported.metricType.orElse(this.metricType),
      default = imported.default.orElse(this.default),
      tags = imported.tags.orElse(this.tags),
      trim = imported.trim.orElse(this.trim),
      foreignKey = imported.foreignKey.orElse(this.foreignKey),
      ignore = imported.ignore.orElse(this.ignore),
      accessPolicy = imported.accessPolicy.orElse(this.accessPolicy)
    )
  }

  def mergeAttrList(ymlAttrs: List[Attribute]): List[Attribute] = {
    this.attributes.getOrElse(Nil).map { xsdAttr =>
      ymlAttrs.find(_.name == xsdAttr.name) match {
        case Some(ymlAttr) if xsdAttr.`type` == "struct" =>
          assert(
            ymlAttr.`type` == "struct",
            s"attribute with name ${ymlAttr.name} found with type ${ymlAttr.`type`} where type 'struct' is expected"
          )
          xsdAttr.importAttr(ymlAttr)
        case Some(ymlAttr) =>
          xsdAttr.importAttributeProperties(ymlAttr)
        case None =>
          xsdAttr
      }
    }
  }

  def importAttr(ymlAttr: Attribute): Attribute = {
    val merged = this.importAttributeProperties(ymlAttr)
    (ymlAttr.attributes, this.attributes) match {
      case (Some(ymlSubAttrs), Some(xsdSubAttrs)) =>
        val mergedAttributes = this.mergeAttrList(ymlSubAttrs)
        merged.copy(attributes = Some(mergedAttributes))
      case (None, None) => merged
      case (_, _) =>
        throw new Exception(
          s"XSD and YML sources contradict each other for attributes ${this.name} && ${ymlAttr.name}"
        )
    }
  }

  override def toString: String =
    // we pretend the "settings" field does not exist
    s"Attribute(${name},${`type`},${array},${required},${getPrivacy()},${comment},${rename},${metricType},${attributes},${position},${default},${tags})"

  /** Check attribute validity An attribute is valid if :
    *   - Its name is a valid identifier
    *   - its type is defined
    *   - When a privacy function is defined its primitive type is a string
    *
    * @return
    *   true if attribute is valid
    */
  def checkValidity(schemaHandler: SchemaHandler): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    if (`type` == null)
      errorList += s"$this : unspecified type"

    val colNamePattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]{1,767}")
    // if (!colNamePattern.matcher(name).matches())
    //  errorList += s"attribute with name $name should respect the pattern ${colNamePattern.pattern()}"

    if (!rename.forall(colNamePattern.matcher(_).matches()))
      errorList += s"renamed attribute with renamed name '$rename' should respect the pattern ${colNamePattern.pattern()}"

    val primitiveType = `type`(schemaHandler).map(_.primitiveType)

    primitiveType match {
      case Some(tpe) =>
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
        `type`(schemaHandler).foreach { someTpe =>
          Try(someTpe.sparkValue(default)) match {
            case Success(_) =>
            case Failure(e) =>
              errorList += s"attribute with name $name: Invalid default value for this attribute type ${e.getMessage()}"
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

    (script, required) match {
      case (Some(_), true) =>
        logger.warn(
          s"Attribute $name : Scripted attributed cannot be required. It will be forced to optional"
        )
      case (_, _) =>
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def `type`(schemaHandler: SchemaHandler): Option[Type] =
    schemaHandler.types().find(_.name == `type`)

  /** Spark Type if this attribute is a primitive type of array of primitive type
    *
    * @return
    *   Primitive type if attribute is a leaf node or array of primitive type, None otherwise
    */
  def primitiveSparkType(schemaHandler: SchemaHandler): DataType = {
    `type`(schemaHandler)
      .map { tpe =>
        if (isArray())
          ArrayType(tpe.primitiveType.sparkType(tpe.zone), !required)
        else
          tpe.primitiveType.sparkType(tpe.zone)
      }
      .getOrElse(PrimitiveType.struct.sparkType(None))
  }

  /** Go get recursively the Spark tree type of this object
    *
    * @return
    *   Spark type of this attribute
    */
  def sparkType(schemaHandler: SchemaHandler): DataType = {
    val tpe = primitiveSparkType(schemaHandler)
    tpe match {
      case _: StructType =>
        attributes.map { attrs =>
          val fields = attrs.map { attr =>
            StructField(attr.name, attr.sparkType(schemaHandler), !attr.required)
          }
          if (isArray())
            ArrayType(StructType(fields))
          else
            StructType(fields)
        } getOrElse (throw new Exception("Should never happen: empty list of attributes"))

      case simpleType => simpleType
    }
  }

  def ddlMapping(
    isPrimaryKey: Boolean,
    datawarehouse: String,
    schemaHandler: SchemaHandler
  ): DDLField = {
    attributes match {
      // TODO: Support NoSQL Documents
      case Some(attrs) =>
        DDLNode(
          this.getFinalName(),
          attrs.map(_.ddlMapping(false, datawarehouse, schemaHandler)),
          required,
          isArray(),
          comment,
          Utils.labels(tags)
        )
      case None =>
        `type`(schemaHandler).map { tpe =>
          tpe.ddlMapping match {
            case None => throw new Exception(s"No mapping found for type $tpe")
            case Some(mapping) =>
              DDLLeaf(
                this.getFinalName(),
                mapping(datawarehouse),
                this.required,
                this.comment,
                isPrimaryKey,
                Utils.labels(tags)
              )
          }
        } getOrElse (throw new Exception(s"Unknown type ${`type`}"))
    }
  }
  def indexMapping(schemaHandler: SchemaHandler): String = {
    attributes match {
      case Some(attrs) =>
        s"""
           |"$name": {
           |  "properties" : {
           |  ${attrs.map(_.indexMapping(schemaHandler)).mkString(",")}
           |  }
           |}""".stripMargin

      case None =>
        `type`(schemaHandler).map { tpe =>
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
                case "epoch_milli"  => Some("epoch_millis")
                case "epoch_second" => Some("epoch_second")
                case x if PrimitiveType.dateFormatters.keys.exists(_ == x) =>
                  Some("date") // ISO formatters are now supported by ES
                case y => None // unknown to ES
              }
              format match {
                case Some(_) =>
                  // "format" : "$fmt"
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

  /** @return
    *   renamed column if defined, source name otherwise
    */
  @JsonIgnore
  def getFinalName(): String = rename.getOrElse(name)

  def isIgnore(): Boolean = ignore.getOrElse(false)

  def getPrivacy(): PrivacyLevel = Option(privacy).getOrElse(PrivacyLevel.None)

  def isArray(): Boolean = array.getOrElse(false)

  def isRequired(): Boolean = Option(required).getOrElse(false)

  @JsonIgnore
  val transform: Option[String] = Option(privacy).filter(_.sql).map(_.value)

  @JsonIgnore
  def getMetricType(schemaHandler: SchemaHandler): MetricType = {
    val sparkType = `type`(schemaHandler).map(tpe => tpe.primitiveType.sparkType(tpe.zone))
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

object Attribute {
  def apply(sparkField: StructField): Attribute = {
    val sparkType = sparkField.dataType
    val fieldName = sparkField.name
    val required = !sparkField.nullable
    val isArray = sparkType.isInstanceOf[ArrayType]
    sparkType match {
      case _: StructType =>
        val struct = sparkType.asInstanceOf[StructType]
        val subFields = struct.fields.map(field => apply(field))
        new Attribute(
          fieldName,
          PrimitiveType.struct.toString,
          Some(isArray),
          required,
          attributes = Some(subFields.toList)
        )
      case _ =>
        val tpe = PrimitiveType.from(sparkType)
        new Attribute(fieldName, tpe.toString, Some(isArray), required)
    }
  }
}
