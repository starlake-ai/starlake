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
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.JsonIgnore
import com.github.ghik.silencer.silent
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Let's say you are willing to import customers and orders from your Sales system. Sales is
  * therefore the domain and customer & order are your datasets. In a DBMS, A Domain would be
  * implemented by a DBMS schema and a dataset by a DBMS table. In BigQuery, The domain name would
  * be the Big Query dataset name and the dataset would be implemented by a Big Query table.
  *
  * @param name
  *   Domain name. Make sure you use a name that may be used as a folder name on the target storage.
  *   - When using HDFS or Cloud Storage, files once ingested are stored in a sub-directory named
  *     after the domain name.
  *   - When used with BigQuery, files are ingested and sorted in tables under a dataset named after
  *     the domain name.
  * @param directory
  *   : Folder on the local filesystem where incoming files are stored. Typically, this folder will
  *   be scanned periodically to move the dataset to the cluster for ingestion. Files located in
  *   this folder are moved to the pending folder for ingestion by the "import" command.
  * @param metadata
  *   : Default Schema metadata. This metadata is applied to the schemas defined in this domain.
  *   Metadata properties may be redefined at the schema level. See Metadata Entity for more
  *   details.
  * @param tables
  *   : List of schemas for each dataset in this domain A domain ususally contains multiple schemas.
  *   Each schema defining how the contents of the input file should be parsed. See Schema for more
  *   details.
  * @param comment
  *   : Domain Description (free text)
  * @param extensions
  *   : recognized filename extensions. json, csv, dsv, psv are recognized by default Only files
  *   with these extensions will be moved to the pending folder.
  * @param ack
  *   : Ack extension used for each file. ".ack" if not specified. Files are moved to the pending
  *   folder only once a file with the same name as the source file and with this extension is
  *   present. To move a file without requiring an ack file to be present, set explicitly this
  *   property to the empty string value "".
  */
@silent case class Domain(
  name: String,
  @silent @deprecated("Moved to Metadata", "0.2.8") directory: Option[String] = None,
  metadata: Option[Metadata] = None,
  tableRefs: Option[List[String]] = None,
  tables: List[Schema] = Nil,
  comment: Option[String] = None,
  @silent @deprecated("Moved to Metadata", "0.2.8") extensions: Option[List[String]] = None,
  @silent @deprecated("Moved to Metadata", "0.2.8") ack: Option[String] = None,
  tags: Option[Set[String]] = None,
  rename: Option[String] = None
) {

  /** @return
    *   renamed column if defined, source name otherwise
    */
  @JsonIgnore
  def getFinalName(): String = rename.getOrElse(name)

  /** Get schema from filename Schema are matched against filenames using filename patterns. The
    * schema pattern that matches the filename is returned
    *
    * @param filename
    *   : dataset filename
    * @return
    */
  def findSchema(filename: String): Option[Schema] = {
    tables.find(_.pattern.matcher(filename).matches())
  }

  def ddlMapping(datawarehouse: String, ddlType: String)(implicit
    settings: Settings
  ): (Path, String) = {
    val rootPath = new Path(new Path(DatasetArea.mapping, "ddl"), datawarehouse)
    val mustache = new Path(rootPath, s"$ddlType.mustache")
    val ssp = new Path(rootPath, s"$ddlType.ssp")
    val template =
      if (settings.metadataStorageHandler.exists(mustache))
        mustache
      else if (settings.metadataStorageHandler.exists(ssp))
        ssp
      else
        throw new Exception(s"No $ddlType.mustache/ssp found for datawarehouse $datawarehouse")
    template -> settings.metadataStorageHandler.read(template)
  }

  /** Load Elasticsearch template file if it exist
    *
    * @param schema
    *   : Schema name to map to an elasticsearch index
    * @return
    *   ES template with optionally the __PROPERTIES__ string that will be replaced by the schema
    *   attributes dynamically computed mappings
    */
  def esMapping(
    schema: Schema
  )(implicit settings: Settings): Option[String] = {
    val template = new Path(new Path(DatasetArea.mapping, this.name), schema.name + ".json")
    if (settings.metadataStorageHandler.exists(template))
      Some(settings.metadataStorageHandler.read(template))
    else
      None
  }

  /** List of file extensions to scan for in the domain directory
    *
    * @param defaultFileExtensions
    *   List of comma separated accepted file extensions
    * @return
    *   the list of extensions of teh default ones : ".json", ".csv", ".dsv", ".psv"
    */
  def getExtensions(defaultFileExtensions: String, forceFileExtensions: String): List[String] = {
    def toList(extensions: String) = extensions.split(',').map(_.trim).toList
    val allExtensions =
      resolveExtensions().getOrElse(toList(defaultFileExtensions)) ++ toList(forceFileExtensions)
    allExtensions.distinct.map { extension =>
      if (extension.startsWith(".") || extension.isEmpty)
        extension
      else
        "." + extension
    }
  }

  /** Resolve method are here to handle backward compatibility
    * @return
    */
  @silent def resolveDirectory(): String = {
    val maybeDirectory = for {
      metadata  <- metadata
      directory <- metadata.directory
    } yield directory

    maybeDirectory match {
      case Some(directory) => directory
      case None =>
        this.directory
          .getOrElse(throw new Exception("directory attribute is mandatory. should never happen"))
    }
  }

  @silent def resolveAck(): Option[String] = {
    val maybeAck = for {
      metadata <- metadata
      ack      <- metadata.ack
    } yield ack

    maybeAck match {
      case Some(ack) => maybeAck
      case None      => this.ack
    }
  }

  @silent def resolveExtensions(): Option[List[String]] =
    metadata.map(m => m.extensions).getOrElse(this.extensions)

  /** Ack file should be present for each file to ingest.
    *
    * @return
    *   the ack attribute or ".ack" by default
    */
  def getAck(): String = resolveAck().map(ack => if (ack.nonEmpty) "." + ack else ack).getOrElse("")

  /** Is this Domain valid ? A domain is valid if :
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

    Try(resolveDirectory()) match {
      case Success(_) => //
      case Failure(_) =>
        errorList += s"Domain with name $name should define the directory attribute"
    }

    // Check Schemas validity
    tables.foreach { schema =>
      for (errors <- schema.checkValidity(this.metadata, schemaHandler).left) {
        errorList ++= errors.map(s"schema ${schema.name}:" + _)
      }
    }

    // Check Metadata validity
    metadata.foreach { metadata =>
      for (errors <- metadata.checkValidity(schemaHandler).left) {
        errorList ++= errors.map(s"domain $name:" + _)
      }
    }

    val duplicatesErrorMessage =
      "Schema %s defined %d times. A schema can only be defined once."
    for (errors <- Utils.duplicates(tables.map(_.name), duplicatesErrorMessage).left) {
      errorList ++= errors.map(s"domain $name:" + _)
    }

    // TODO Check partition columns
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def asDot(includeAllAttrs: Boolean): String = {
    tables
      .map { schema =>
        schema.asDot(name, includeAllAttrs)
      }
      .mkString("\n")
  }

  def policies(): List[RowLevelSecurity] = {
    tables
      .flatMap(_.acl.getOrElse(Nil))
      .map(ace => RowLevelSecurity(name = ace.role, grants = ace.grants.toSet)) ++
    tables.flatMap(
      _.rls.getOrElse(Nil)
    )
  }
}
