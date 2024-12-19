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

package ai.starlake.schema.handlers

import ai.starlake.config.Settings
import ai.starlake.schema.exceptions.InvalidFieldNameException
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern
import scala.collection.GenSeq
import scala.util.Try

object InferSchemaHandler {
  val datePattern = "[0-9]{4}-[0-9]{2}-[0-9]{2}".r.pattern

  def parseIsoInstant(str: String): Boolean = {
    try {
      ZonedDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME)
      true
    } catch {
      case e: java.time.format.DateTimeParseException => false
    }
  }

  val identifierRegex = "^([a-zA-Z_][a-zA-Z\\d_:.-]*)$".r

  def convertToValidXMLSchema(
    currentSchema: DataType
  ): DataType = {
    val result =
      currentSchema match {
        case st: StructType =>
          val updatedFields = st.fields.map { originalField =>
            val updatedField =
              originalField.copy(name = originalField.name.replaceAll("[:.-]", "_"))
            updatedField.dataType match {
              case st: StructType =>
                updatedField.copy(dataType = convertToValidXMLSchema(st))
              case dt: ArrayType =>
                updatedField.copy(dataType =
                  dt.copy(elementType = convertToValidXMLSchema(dt.elementType))
                )
              case _ => updatedField
            }
          }
          st.copy(fields = updatedFields)
        case dt: ArrayType =>
          dt.copy(elementType = convertToValidXMLSchema(dt.elementType))
        // if the datatype is a simple Attribute
        case simpleType =>
          simpleType
      }
    result
  }

  /** * Traverses the schema and returns a list of attributes.
    *
    * @param schema
    *   Schema so that we find all Attributes
    * @return
    *   List of Attributes
    */
  def createAttributes(
    lines: List[Row],
    schema: StructType,
    format: Format,
    forcePattern: Boolean = true
  ): List[Attribute] = {

    def createAttribute(
      currentLines: List[Any],
      currentSchema: DataType,
      container: StructField,
      fieldPath: String,
      forcePattern: Boolean = true
    ): Attribute = {
      val result =
        currentSchema match {
          case st: StructType =>
            val schemaWithIndex: Seq[(StructField, Int)] = st.zipWithIndex
            val rename = container.name.replaceAll("[:.-]", "_")
            val renamedField = if (rename != container.name) Some(rename) else None
            try {
              val attributes = schemaWithIndex.map { case (field, index) =>
                val structLines =
                  currentLines.flatMap(Option(_)).map {
                    case r: Row => r.get(index)
                    case other =>
                      throw new RuntimeException(
                        "Encountered " + other.getClass.getName + s" for field path $fieldPath but expected a Row instead for a Struct"
                      )
                  }

                createAttribute(
                  structLines,
                  st(index).dataType,
                  field,
                  fieldPath + "." + field.name,
                  forcePattern
                )
              }.toList

              Attribute(
                name = container.name,
                `type` = st.typeName,
                rename = renamedField,
                required = if (!container.nullable) Some(true) else None,
                array = Some(false),
                attributes = attributes,
                sample = None // currentLines.map(Option(_)).headOption.flatten.map(_.toString)
              )
            } catch {
              case _: InvalidFieldNameException =>
                Attribute(
                  name = container.name,
                  `type` = PrimitiveType.variant.value,
                  rename = renamedField,
                  required = if (!container.nullable) Some(true) else None,
                  array = Some(false),
                  attributes = Nil,
                  sample = None // currentLines.map(Option(_)).headOption.flatten.map(_.toString)
                )
            }
          case dt: ArrayType =>
            dt.elementType match {
              case _: ArrayType =>
                throw new RuntimeException(
                  s"Starlake doesn't support array of array. Rejecting field path $fieldPath"
                )
              case _ =>
                // if the array contains elements of type struct.
                // {people: [{name:Person1, age:22},{name:Person2, age:25}]}
                val arrayLines =
                  currentLines.flatMap(Option(_)).flatMap {
                    case ar: GenSeq[_] => ar
                    case other =>
                      throw new RuntimeException(
                        s"Expecting an array to be contained into a Seq for field path $fieldPath and not ${other.getClass.getName}"
                      )
                  }

                val stAttributes = createAttribute(
                  arrayLines,
                  dt.elementType,
                  container,
                  fieldPath + "[]",
                  forcePattern
                )
                stAttributes.copy(
                  array = Some(true),
                  required = if (!container.nullable) Some(true) else None,
                  sample = None // currentLines.map(Option(_)).headOption.flatten.map(_.toString)
                )
            }
          // if the datatype is a simple Attribute
          case _ =>
            val cellType = currentSchema.typeName match {
              case "string" =>
                val timestampCandidates = currentLines.flatMap(Option(_).map(_.toString))
                if (timestampCandidates.isEmpty)
                  "string"
                else if (timestampCandidates.forall(v => parseIsoInstant(v)))
                  "iso_date_time"
                else if (timestampCandidates.forall(v => datePattern.matcher(v).matches()))
                  "date"
                else
                  "string"
              case "timestamp"
                  if Set(
                    Format.DSV,
                    Format.POSITION,
                    Format.JSON_FLAT
                  ).contains(format) =>
                // We handle here the case when it is a date and not a timestamp
                val timestamps = currentLines.flatMap(Option(_).map(_.toString))
                if (timestamps.forall(v => datePattern.matcher(v).matches()))
                  "date"
                else if (timestamps.forall(v => parseIsoInstant(v)))
                  "iso_date_time"
                else
                  "timestamp"
              case _ =>
                PrimitiveType.from(currentSchema).value
            }
            val rename = container.name.replaceAll("[:.-]", "_")
            val renamedField = if (rename != container.name) Some(rename) else None
            Attribute(
              name = container.name,
              `type` = cellType,
              rename = renamedField,
              array = Some(false),
              required = if (!container.nullable) Some(true) else None,
              sample = currentLines.map(Option(_)).headOption.flatten.map(_.toString)
            )
        }

      if (!forcePattern || identifierRegex.pattern.matcher(result.name).matches)
        result
      else
        throw new InvalidFieldNameException(result.name)
    }
    createAttribute(
      lines,
      schema,
      StructField("_ROOT_", StructType(Nil)),
      "",
      forcePattern
    ).attributes
  }

  /** * builds the Metadata case class. check case class metadata for attribute definition
    *
    * @param format
    *   : DSV by default
    * @param array
    *   : Is a json stored as a single object array ? false by default
    * @param withHeader
    *   : does the dataset has a header ? true bu default
    * @param separator
    *   : the column separator, ';' by default
    * @return
    */

  def createMetaData(
    format: Format,
    array: Option[Boolean] = None,
    withHeader: Option[Boolean] = None,
    separator: Option[String] = None,
    options: Option[Map[String, String]] = None
  ): Metadata =
    Metadata(
      format = Some(format),
      encoding = None,
      multiline = None,
      array = if (array.contains(true)) array else None,
      withHeader = withHeader,
      separator = separator,
      options = options
    )

  /** * builds the Schema case class
    *
    * @param name
    *   : Schema name, must be unique in the domain. Will become the hive table name
    * @param pattern
    *   : filename pattern to which this schema must be applied
    * @param attributes
    *   : datasets columns
    * @param metadata
    *   : Dataset metadata
    * @return
    */

  def createSchema(
    name: String,
    pattern: Pattern,
    comment: Option[String],
    attributes: List[Attribute],
    metadata: Option[Metadata],
    sample: Option[String]
  ): Schema =
    Schema(
      name = name,
      pattern = pattern,
      attributes = attributes,
      metadata = metadata,
      comment = comment,
      sample = sample
    )

  /** * Builds the Domain case class
    *
    * @param name
    *   : Domain name
    * @param directory
    *   : Folder on the local filesystem where incomping files are stored. This folder will be
    *   scanned regurlaly to move the dataset to the cluster
    * @param metadata
    *   : Default Schema meta data.
    * @param schemas
    *   : List of schema for each dataset in this domain
    * @return
    */

  def createDomain(
    name: String,
    metadata: Option[Metadata] = None,
    schemas: List[Schema] = Nil
  ): Domain =
    Domain(name = name, metadata = metadata, tables = schemas)

  /** * Generates the YAML file using the domain object and a savepath
    *
    * @param domain
    *   Domain case class
    * @param savePath
    *   path to save files.
    */
  def generateYaml(domain: Domain, saveDir: String, clean: Boolean)(implicit
    settings: Settings
  ): Try[Path] = Try {
    implicit val storageHandler: StorageHandler = settings.storageHandler()

    /** load: metadata: directory: "{{incoming_path}}"
      */
    val domainFolder = new Path(saveDir, domain.name)
    storageHandler.mkdirs(domainFolder)
    val configPath = new Path(domainFolder, "_config.sl.yml")
    if (!storageHandler.exists(configPath)) {
      YamlSerde.serializeToPath(configPath, domain.copy(tables = Nil))
    }
    val table = domain.tables.head
    val tablePath = new Path(domainFolder, s"${table.name}.sl.yml")
    if (storageHandler.exists(tablePath) && !clean) {
      throw new Exception(
        s"Could not write table ${domain.tables.head.name} already defined in file $tablePath"
      )
    }
    YamlSerde.serializeToPath(tablePath, table)
    tablePath
  }
}
