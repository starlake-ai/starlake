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

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.DataTypeEx._
import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._

import java.util.regex.Pattern
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import ai.starlake.schema.model.Severity._

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
  attributes: List[Attribute] = Nil,
  position: Option[Position] = None,
  default: Option[String] = None,
  tags: Set[String] = Set.empty,
  trim: Option[Trim] = None,
  script: Option[String] = None,
  foreignKey: Option[String] = None, // [domain.]table.attribute
  ignore: Option[Boolean] = None,
  accessPolicy: Option[String] = None
) extends Named
    with LazyLogging {
  def this() = this("") // Should never be called. Here for Jackson deserialization only

  override def toString: String =
    // we pretend the "settings" field does not exist
    s"Attribute(${name},${`type`},${array},${required},${getPrivacy()},${comment},${rename},${metricType},${attributes},${position},${default},${tags})"

  @JsonIgnore
  def isNestedOrRepeatedField(): Boolean = {
    attributes.nonEmpty || array.getOrElse(false)
  }

  /** Check attribute validity An attribute is valid if :
    *   - Its name is a valid identifier
    *   - its type is defined
    *   - When a privacy function is defined its primitive type is a string
    *
    * @return
    *   true if attribute is valid
    */
  def checkValidity(schemaHandler: SchemaHandler): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.MutableList[ValidationMessage] = mutable.MutableList.empty
    if (`type` == null)
      errorList +=
        ValidationMessage(Error, "Attribute.type", s"$this : unspecified type")

    val colNamePattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]{1,767}")
    // We do not check if the name is valid. We only need to check that once renamed, the name is valid.
    if (!rename.forall(colNamePattern.matcher(_).matches()))
      errorList += ValidationMessage(
        Error,
        "Attribute.rename",
        s"rename: renamed attribute with renamed name '$rename' should respect the pattern ${colNamePattern.pattern()}"
      )

    val primitiveType = `type`(schemaHandler).map(_.primitiveType)

    primitiveType match {
      case Some(tpe) =>
        if (tpe == PrimitiveType.struct && attributes.isEmpty)
          errorList += ValidationMessage(
            Error,
            "Attribute.primitiveType",
            s"Attribute $this : Struct types must have at least one attribute."
          )
        if (tpe != PrimitiveType.struct && attributes.nonEmpty)
          errorList += ValidationMessage(
            Error,
            "Attribute.primitiveType",
            s" $this : Simple attributes cannot have sub-attributes"
          )
      case None if attributes.isEmpty =>
        errorList +=
          ValidationMessage(
            Error,
            "Attribute.primitiveType",
            s"Invalid Type ${`type`}"
          )
      case _ => // good boy
    }

    default.foreach { default =>
      if (required)
        errorList += ValidationMessage(
          Error,
          "Attribute.default",
          s"attribute with name $name - default value valid for optional fields only"
        )
      primitiveType.foreach { primitiveType =>
        if (primitiveType == PrimitiveType.struct)
          errorList += ValidationMessage(
            Error,
            "Attribute.default",
            s"attribute with name $name - default value not valid for struct type fields"
          )
        `type`(schemaHandler).foreach { someTpe =>
          Try(someTpe.sparkValue(default)) match {
            case Success(_) =>
            case Failure(e) =>
              errorList += ValidationMessage(
                Error,
                "Attribute.default",
                s"attribute with name $name - Invalid default value for this attribute type ${e.getMessage()}"
              )
          }
        }
      }
      array.foreach { isArray =>
        if (isArray)
          errorList += ValidationMessage(
            Error,
            "Attribute.array",
            s"attribute with name $name: default value not valid for array fields"
          )
      }
    }

    (script, required) match {
      case (Some(_), true) =>
        ValidationMessage(
          Warning,
          "Attribute.script",
          s"script: Attribute $name : Scripted attributed cannot be required. It will be forced to optional"
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

  def primitiveType(schemaHandler: SchemaHandler): Option[PrimitiveType] =
    schemaHandler.types().find(_.name == `type`).map(_.primitiveType)

  def samePrimitiveType(other: Attribute)(implicit schemaHandler: SchemaHandler): Boolean = {
    this.primitiveType(schemaHandler).map(_.value) == other
      .primitiveType(schemaHandler)
      .map(_.value)
  }

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
    def buildStruct(): List[StructField] = {
      if (attributes.isEmpty)
        throw new Exception("Should never happen: empty list of attributes")
      val fields = attributes.map { attr =>
        val structField = StructField(attr.name, attr.sparkType(schemaHandler), !attr.required)
        attr.comment.map(structField.withComment).getOrElse(structField)
      }
      fields
    }

    val tpe = primitiveSparkType(schemaHandler)
    tpe match {
      case ArrayType(s: StructType, b: Boolean) =>
        val fields = buildStruct()
        ArrayType(StructType(fields))
      case _: StructType =>
        val fields = buildStruct()
        StructType(fields)
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
      case attr :: tail =>
        DDLNode(
          this.getFinalName(),
          attributes.map(_.ddlMapping(false, datawarehouse, schemaHandler)),
          required,
          isArray(),
          comment,
          Utils.labels(tags)
        )
      case Nil =>
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
      case attr :: head =>
        s"""
           |"$name": {
           |  "properties" : {
           |  ${attributes.map(_.indexMapping(schemaHandler)).mkString(",")}
           |  }
           |}""".stripMargin

      case Nil =>
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

  def compare(other: Metadata): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(name, this, other)

  def containsArrayOfRecords(): Boolean = {
    val isArrayOfRecords = this.array.getOrElse(false) && this.`type` == "struct"
    if (isArrayOfRecords) {
      true
    } else {
      attributes.exists(_.containsArrayOfRecords())
    }
  }

  def deepForeignKeyForDot(): Option[String] = {
    this.foreignKey match {
      case Some(_) => this.foreignKey
      case None =>
        attributes.flatMap(_.deepForeignKeyForDot()).headOption
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
          attributes = subFields.toList
        )
      case _ =>
        val tpe = PrimitiveType.from(sparkType)
        new Attribute(fieldName, tpe.toString, Some(isArray), required)
    }
  }

  /** Compare refAttr with sourceAttr and reject if emptiness of attributes is not the same.
    */
  private def checkAttributesEmptinessMismatch(refAttr: Attribute, sourceAttr: Attribute): Unit = {
    assert(
      refAttr.attributes.isEmpty == sourceAttr.attributes.isEmpty,
      s"attribute with name ${sourceAttr.name} has mismatch on attributes emptiness"
    )
  }

  /** Compare refAttr with sourceAttr type and reject if containers (struct or array) doesn't match.
    * Furthermore, if it's an array type, check that nested element are the same container type as
    * well recursively.
    */
  private def checkContainerMismatch(refAttr: Attribute, sourceAttr: Attribute)(implicit
    schemaHandler: SchemaHandler
  ): Unit = {
    @tailrec
    def matchContainerType(refDataType: DataType, sourceDataType: DataType): Unit = {
      // check element type
      (refDataType, sourceDataType) match {
        case (_: ArrayType, _) | (_, _: ArrayType) =>
          assert(
            refDataType.getClass.getSimpleName == sourceDataType.getClass.getSimpleName,
            s"attribute with name ${sourceAttr.name} is not in array in both schema"
          )
        case (_: StructType, _) =>
          assert(
            refDataType.getClass.getSimpleName == sourceDataType.getClass.getSimpleName,
            s"attribute with name ${sourceAttr.name} found with type '${sourceAttr.`type`}' where type '${refAttr.`type`}' is expected"
          )
        case _ => // do nothing
      }
      (refDataType, sourceDataType) match {
        case (refArrayType: ArrayType, sourceArrayType: ArrayType) =>
          // check array element type. Can't have mismatch between type since we've checked element type.
          matchContainerType(refArrayType.elementType, sourceArrayType.elementType)
        case _ => // do nothing
      }
    }

    val refAttrDataType = refAttr.primitiveSparkType(schemaHandler)
    val sourceAttrDataType = sourceAttr.primitiveSparkType(schemaHandler)
    matchContainerType(refAttrDataType, sourceAttrDataType)
  }

  private def merge(
    refAttr: Attribute,
    sourceAttr: Attribute,
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit schemaHandler: SchemaHandler): Attribute = {
    if (attributeMergeStrategy.failOnContainerMismatch) {
      checkContainerMismatch(refAttr, sourceAttr)
    }
    if (attributeMergeStrategy.failOnAttributesEmptinessMismatch) {
      checkAttributesEmptinessMismatch(refAttr, sourceAttr)
    }
    val refAttrDataType = refAttr.primitiveSparkType(schemaHandler)
    val sourceAttrDataType = sourceAttr.primitiveSparkType(schemaHandler)
    val attributes = mergeContainerAttributes(
      refAttr,
      sourceAttr,
      attributeMergeStrategy,
      refAttrDataType,
      sourceAttrDataType
    )
    val (referenceSource, fallbackSource) =
      attributeMergeStrategy.attributePropertiesMergeStrategy match {
        case RefFirst    => refAttr    -> sourceAttr
        case SourceFirst => sourceAttr -> refAttr
      }
    refAttr.copy(
      privacy =
        if (referenceSource.privacy != PrivacyLevel.None) referenceSource.privacy
        else fallbackSource.privacy, // We currently have no way to
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
    refAttr: Attribute,
    sourceAttr: Attribute,
    attributeMergeStrategy: AttributeMergeStrategy,
    refAttrDataType: DataType,
    sourceAttrDataType: DataType
  )(implicit schemaHandler: SchemaHandler) = {
    val attributes = (refAttrDataType, sourceAttrDataType) match {
      case (_: StructType, _: StructType) =>
        mergeAll(
          refAttr.attributes,
          sourceAttr.attributes,
          attributeMergeStrategy
        )
      case (_: StructType, _) =>
        // Since we would have already failed on container mismatch, this indicates that we keep all attributes from ref only because we can't merge them.
        mergeAll(
          refAttr.attributes,
          Nil,
          attributeMergeStrategy
        )
      case (refType: ArrayType, sourceType: ArrayType) =>
        (refType.elementType, sourceType.elementType) match {
          case (_: StructType, _: StructType) =>
            mergeAll(
              refAttr.attributes,
              sourceAttr.attributes,
              attributeMergeStrategy
            )
          case (_: StructType, _) =>
            // Since we would have already failed on container mismatch, this indicates that we keep all attributes from ref only because we can't merge them.
            mergeAll(
              refAttr.attributes,
              Nil,
              attributeMergeStrategy
            )
          case _ =>
            Nil
        }
      case (refType: ArrayType, _) =>
        // Since we would have already failed on container mismatch, this indicates that we keep all attributes from ref only because we can't merge them.
        refType.elementType match {
          case _: StructType => mergeAll(refAttr.attributes, Nil, attributeMergeStrategy)
          case _             => Nil
        }
      case _ =>
        Nil
    }
    attributes
  }

  /** @param refAttrs
    *   attributes to enrich
    * @param sourceAttrs
    *   attributes used to enrich refAttrs
    * @return
    */
  def mergeAll(
    refAttrs: List[Attribute],
    sourceAttrs: List[Attribute],
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit schemaHandler: SchemaHandler): List[Attribute] = {
    val missingAttributes = attributeMergeStrategy.keepSourceDiffAttributesStrategy match {
      case KeepAllDiff =>
        val missingAttributeNames = sourceAttrs.map(_.name).diff(refAttrs.map(_.name))
        missingAttributeNames.flatMap(attrName => sourceAttrs.find(_.name == attrName))
      case KeepOnlyScriptDiff =>
        val missingScriptAttributeNames =
          sourceAttrs.filter(_.script.isDefined).map(_.name).diff(refAttrs.map(_.name))
        missingScriptAttributeNames.flatMap(attrName => sourceAttrs.find(_.name == attrName))
      case DropAll => Nil
    }
    refAttrs.map { refAttr =>
      sourceAttrs.find(_.name == refAttr.name) match {
        case Some(sourceAttr) =>
          merge(refAttr, sourceAttr, attributeMergeStrategy)
        case None =>
          refAttr
      }
    } ++ missingAttributes
  }
}
