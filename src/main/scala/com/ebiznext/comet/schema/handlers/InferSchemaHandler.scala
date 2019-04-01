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

package com.ebiznext.comet.schema.handlers
import java.io.File
import java.util.regex.Pattern
import com.ebiznext.comet.job.Main
import com.ebiznext.comet.schema.model._
import org.apache.spark.sql.types.{ArrayType, StructType}

object InferSchemaHandler {

  /***
    *   Traverses the schema and returns a list of attributes.
    * @param schema Schema so that we find all Attributes
    * @return List of Attributes
    */
  def createAttributes(schema: StructType): List[Attribute] = {
    schema
      .map(
        row =>
          row.dataType.typeName match {

            // if the datatype is a struct {...} containing one or more other field
            case "struct" =>
              Attribute(
                row.name,
                row.dataType.typeName,
                Some(false),
                !row.nullable,
                attributes = Some(createAttributes(row.dataType.asInstanceOf[StructType]))
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
                  attributes = Some(createAttributes(elemType.asInstanceOf[StructType]))
                )
              else
                // if it is a regular array. {ages: [21, 25]}
                Attribute(row.name, elemType.typeName, Some(true), !row.nullable)

            // if the datatype is a simple Attribute
            case _ => Attribute(row.name, row.dataType.typeName, Some(false), !row.nullable)
        }
      )
      .toList
  }

  /***
    *   builds the Metadata case class. check case class metadata for attribute definition
    * @param mode
    * @param format
    * @param multiline
    * @param array
    * @param withHeader
    * @param separator
    * @param quote
    * @param escape
    * @param write
    * @param partition
    * @param index
    * @param mapping
    * @return
    */
  // todo can we replace mode, format and writemode by values instead of options of strings
  def createMetaData(
    mode: Option[String] = None,
    format: Option[String] = None,
    multiline: Option[Boolean] = None,
    array: Option[Boolean],
    withHeader: Option[Boolean],
    separator: Option[String],
    quote: Option[String],
    escape: Option[String],
    write: Option[String] = None,
    partition: Option[Partition] = None,
    index: Option[Boolean] = None,
    mapping: Option[EsMapping] = None
  ): Metadata = {
    Metadata(
      Some(Mode.fromString(mode.getOrElse("FILE"))),
      Some(Format.fromString(format.getOrElse("DSV"))),
      multiline,
      array,
      withHeader,
      separator,
      quote,
      escape,
      Some(WriteMode.fromString(write.getOrElse("APPEND"))),
      partition,
      index,
      mapping
    )
  }

  /***
    *   builds the Schema case class
    * @param name
    * @param pattern
    * @param attributes
    * @param metadata
    * @param merge
    * @param comment
    * @param presql
    * @param postsql
    * @return
    */

  def createSchema(
    name: String,
    pattern: Pattern,
    attributes: List[Attribute],
    metadata: Option[Metadata],
    merge: Option[MergeOptions] = None,
    comment: Option[String] = None,
    presql: Option[List[String]] = None,
    postsql: Option[List[String]] = None
  ): Schema = {

    Schema(name, pattern, attributes, metadata, merge, comment, presql, postsql)
  }

  /***
    * Builds the Domain case class
    * @param name
    * @param directory
    * @param metadata
    * @param schemas
    * @param comment
    * @param extensions
    * @param ack
    * @return
    */

  def createDomain(
    name: String,
    directory: String,
    metadata: Option[Metadata] = None,
    schemas: List[Schema] = Nil,
    comment: Option[String] = None,
    extensions: Option[List[String]] = None,
    ack: Option[String] = None
  ): Domain = {

    Domain(name, directory, metadata, schemas, comment, extensions, ack)
  }

  /***
    * Generates the YAML file using the domain object and a savepath
    * @param domain Domain case class
    * @param savePath path to save files.
    */
  def generateYaml(domain: Domain, savePath: String): Unit = {
    Main.mapper.writeValue(new File(savePath), domain)
  }

}
