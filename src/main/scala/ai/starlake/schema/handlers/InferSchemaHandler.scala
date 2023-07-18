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
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerializer
import better.files.File
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import java.util.regex.Pattern
import scala.util.{Failure, Success, Try}

object InferSchemaHandler {
  val datePattern = "[0-9]{4}-[0-9]{2}-[0-9]{2}".r.pattern

  /** * Traverses the schema and returns a list of attributes.
    *
    * @param schema
    *   Schema so that we find all Attributes
    * @return
    *   List of Attributes
    */
  def createAttributes(
    lines: List[Array[String]],
    schema: StructType,
    format: Format
  )(implicit settings: Settings): List[Attribute] = {
    val schemaWithIndex: Seq[(StructField, Int)] = schema.zipWithIndex

    schemaWithIndex
      .map { case (row, index) =>
        row.dataType.typeName match {

          // if the datatype is a struct {...} containing one or more other field
          case "struct" =>
            Attribute(
              row.name,
              row.dataType.typeName,
              Some(false),
              !row.nullable,
              attributes = createAttributes(lines, row.dataType.asInstanceOf[StructType], format)
            )

          case "array" =>
            val elemType = row.dataType.asInstanceOf[ArrayType].elementType
            if (elemType.typeName.equals("struct"))
              // if the array contains elements of type struct.
              // {people: [{name:Person1, age:22},{name:Person2, age:25}]}
              Attribute(
                row.name,
                elemType.typeName,
                Some(true),
                !row.nullable,
                attributes = createAttributes(lines, elemType.asInstanceOf[StructType], format)
              )
            else
              // if it is a regular array. {ages: [21, 25]}
              Attribute(row.name, elemType.typeName, Some(true), !row.nullable)

          // if the datatype is a simple Attribute
          case _ =>
            val cellType =
              if (
                row.dataType.typeName == "timestamp" && Set(
                  Format.DSV,
                  Format.POSITION,
                  Format.SIMPLE_JSON
                ).contains(format)
              ) {
                // We handle here the case when it is a date and not a timestamp
                val timestamps = lines.map(row => row(index)).flatMap(Option(_))
                if (timestamps.forall(v => datePattern.matcher(v).matches()))
                  "date"
                else
                  "timestamp"
              } else
                row.dataType.typeName
            Attribute(row.name, cellType, Some(false), !row.nullable)
        }
      }
  }.toList

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
    separator: Option[String] = None
  ): Metadata =
    Metadata(
      mode = Some(Mode.fromString("FILE")),
      format = Some(format),
      encoding = None,
      multiline = None,
      array = array,
      withHeader = withHeader,
      separator = separator
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
    attributes: List[Attribute],
    metadata: Option[Metadata]
  ): Schema =
    Schema(
      name = name,
      pattern = pattern,
      attributes = attributes,
      metadata = metadata,
      None,
      None,
      Nil,
      Nil
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
  def generateYaml(domain: Domain, saveDir: String)(implicit
    settings: Settings
  ): Try[File] = Try {
    val savePath = File(saveDir, s"${domain.name}.comet.yml")
    savePath.parent.createDirectories()
    if (savePath.exists) {

      val existingDomain = YamlSerializer.deserializeDomain(
        settings.storageHandler().read(new Path(savePath.pathAsString)),
        savePath.pathAsString
      )
      existingDomain match {
        case Success(existingDomain) =>
          val tableExist =
            existingDomain.tables.exists(schema =>
              schema.name.toLowerCase() == domain.tables.head.name
            )
          if (tableExist)
            throw new Exception(
              s"Table ${domain.tables.head.name} already defined in file $savePath"
            )
          val finalDomain =
            existingDomain.copy(tables = existingDomain.tables ++ domain.tables)
          YamlSerializer.serializeToFile(savePath, finalDomain)
        case Failure(e) =>
          e.printStackTrace()
          throw e
      }

    } else {
      YamlSerializer.serializeToFile(savePath, domain)
    }
    savePath
  }
}
