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

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.handlers.SchemaHandler
import org.apache.hadoop.fs.Path

import scala.collection.mutable

/**
  * Let's say you are wiling to import from you Sales system customers and orders.
  * Sales is therefore the domain and Customer & order are your datasets.
  *
  * @param name       : Domain name
  * @param directory  : Folder on the local filesystem where incomping files are stored.
  *                     Typically, this folder will be scanned regurlaly to move the dataset to the cluster for ingestion.
  *                     Files located in this folder are moved to the pending folder for ingestion by the "import" command.
  * @param metadata   : Default Schema metadata.
  *                     This metadata is applied to the schemas defined in this domain.
  *                     Metadata properties may be redefined at the schema level.
  *                     See Metadata Entity for more details.
  * @param schemas    : List of schemas for each dataset in this domain
  *                     A domain ususally contains multiple schemas. Each schema defining how the contents of the input file should be parsed.
  *                     See Schema for more details.
  * @param comment    : Domain Description (free text)
  * @param extensions : recognized filename extensions. json, csv, dsv, psv are recognized by default
  *                     Only files with these extensions will be moved to the pending folder.
  * @param ack        : Ack extension used for each file. ".ack" if not specified.
  *                     Files are moved to the pending folder only once a file with the same name as the source file and with this extension
  *                     is present.
  *                     To move a file without requiring an ack file to be present, set explicitly this property to the empty string value "".
  */
case class Domain(
  name: String,
  directory: String,
  metadata: Option[Metadata] = None,
  schemas: List[Schema] = Nil,
  comment: Option[String] = None,
  extensions: Option[List[String]] = None,
  ack: Option[String] = None
) {

  /**
    * Get schema from filename
    * Schema are matched against filenames using filename patterns.
    * The schema pattern thats matches the filename is returned
    *
    * @param filename : dataset filename
    * @return
    */
  def findSchema(filename: String): Option[Schema] = {
    schemas.find(_.pattern.matcher(filename).matches())
  }

  /**
    * Load Elasticsearch template file if it exist
    *
    * @param schema : Schema name to map to an elasticsearch index
    * @return ES template with optinnaly the __PROPERTIES__ string
    *         that will be replaced by the schema attributes dynamically
    *         computed mappings
    */
  def mapping(
    schema: Schema
  )(implicit settings: Settings): Option[String] = {
    val template = new Path(new Path(DatasetArea.mapping, this.name), schema.name + ".json")
    if (settings.storageHandler.exists(template))
      Some(settings.storageHandler.read(template))
    else
      None
  }

  /**
    * List of file extensions to scan for in the domain directory
    *
    * @return the list of extensions of teh default ones : ".json", ".csv", ".dsv", ".psv"
    */
  def getExtensions(): List[String] = {
    extensions.getOrElse(List("json", "csv", "dsv", "psv")).map("." + _)
  }

  /**
    * Ack file should be present for each file to ingest.
    *
    * @return the ack attribute or ".ack" by default
    */
  def getAck(): String = ack.map(ack => if (ack.nonEmpty) "." + ack else ack).getOrElse(".ack")

  /**
    * Is this Domain valid ? A domain is valid if :
    *   - The domain name is a valid attribute
    *   - all the schemas defined in this domain are valid
    *   - No schema is defined twice
    *   - Partitions columns are valid columns
    *   - The input directory is a valid path
    *
    * @return
    */
  def checkValidity(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    // Check Domain name validity
    val dbNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,100}")
    if (!dbNamePattern.matcher(name).matches())
      errorList += s"Domain with name $name should respect the pattern ${dbNamePattern.pattern()}"

    // Check Schemas validity
    schemas.foreach { schema =>
      for (errors <- schema.checkValidity(this.metadata, schemaHandler).left) {
        errorList ++= errors
      }
    }

    // Check Metadata validity
    metadata.foreach { metadata =>
      for (errors <- metadata.checkValidity(schemaHandler).left) {
        errorList ++= errors
      }
    }

    val duplicatesErrorMessage =
      "%s is defined %d times. A schema can only be defined once."
    for (errors <- duplicates(schemas.map(_.name), duplicatesErrorMessage).left) {
      errorList ++= errors
    }

    // TODO Check partition columns

    // TODO Validate directory
    val inputDir = new Path(this.directory)
    if (!settings.storageHandler.exists(inputDir)) {
      errorList += s"$directory not found"
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }
}
