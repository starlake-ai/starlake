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

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path

import scala.annotation.nowarn
import scala.collection.mutable
import scala.util.Try

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
@nowarn case class Domain(
  name: String,
  @nowarn @deprecated("Moved to Metadata", "0.2.8") directory: Option[String] = None,
  metadata: Option[Metadata] = None,
  tableRefs: List[String] = Nil,
  tables: List[Schema] = Nil, // deprecated("Moved to tableRefs", "0.6.4")
  comment: Option[String] = None,
  @nowarn @deprecated("Moved to Metadata", "0.2.8") extensions: List[String] = Nil,
  @nowarn @deprecated("Moved to Metadata", "0.2.8") ack: Option[String] = None,
  tags: Set[String] = Set.empty,
  rename: Option[String] = None,
  project: Option[String] = None
) extends Named {

  def this() = this("") // Should never be called. Here for Jackson deserialization only

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
      if (settings.storageHandler.exists(mustache))
        mustache
      else if (settings.storageHandler.exists(ssp))
        ssp
      else
        throw new Exception(s"No $ddlType.mustache/ssp found for datawarehouse $datawarehouse")
    template -> settings.storageHandler.read(template)
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
    if (settings.storageHandler.exists(template))
      Some(settings.storageHandler.read(template))
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
    val allExtensions = resolveExtensions() match {
      case Nil  => toList(defaultFileExtensions) ++ toList(forceFileExtensions)
      case list => list
    }
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
  @nowarn def resolveDirectory(): String = {
    resolveDirectoryOpt() match {
      case Some(directory) => directory
      case None => throw new Exception("directory attribute is mandatory. should never happen")
    }
  }

  /** @return
    *   directory if any
    */
  def resolveDirectoryOpt(): Option[String] = {
    val maybeDirectory = for {
      metadata  <- metadata
      directory <- metadata.directory
    } yield directory
    maybeDirectory.orElse(this.directory)
  }

  @nowarn def resolveAck(): Option[String] = {
    val maybeAck = for {
      metadata <- metadata
      ack      <- metadata.ack
    } yield ack

    maybeAck match {
      case Some(ack) => maybeAck
      case None      => this.ack
    }
  }

  @nowarn def resolveExtensions(): List[String] = {
    val ext = metadata.map(m => m.extensions)
    ext.getOrElse(this.extensions)
  }

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
    schemaHandler: SchemaHandler,
    directorySeverity: Severity
  )(implicit settings: Settings): Either[(List[String], List[String]), Boolean] = {

    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val warningList: mutable.MutableList[String] = mutable.MutableList.empty

    // Check Domain name validity
    val forceDomainPrefixRegex = settings.comet.forceDomainPattern.r
    if (!forceDomainPrefixRegex.pattern.matcher(name).matches())
      errorList += s"Domain with name $name should respect the pattern ${forceDomainPrefixRegex.regex}"

    val forceTablePrefixRegex = settings.comet.forceTablePattern.r

    val directoryAssertionOpt = resolveDirectoryOpt() match {
      case Some(_) => None
      case None =>
        Some(
          s"Domain with name $name should define the directory attribute if 'import' command is used."
        )
    }
    directorySeverity match {
      case Error =>
        errorList ++= directoryAssertionOpt
      case Warning => warningList ++= directoryAssertionOpt
      case _       => // do nothing even if directory is not resolved
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
      Left(errorList.toList, warningList.toList)
    else
      Right(true)
  }

  def asDot(includeAllAttrs: Boolean, fkTables: Set[String]): String = {
    tables
      .map { schema =>
        schema.asDot(name, includeAllAttrs, fkTables)
      }
      .mkString("\n")
  }

  def relatedTables(): List[String] = tables.flatMap(_.relatedTables())

  def aclTables(): List[Schema] = tables.filter(_.hasACL())

  def rlsTables(): Map[String, List[RowLevelSecurity]] =
    tables
      .map(t => (t.getFinalName(), t.rls))
      .filter { case (tableName, rls) => rls.nonEmpty }
      .toMap

  def policies(): List[RowLevelSecurity] = {
    tables
      .flatMap(_.acl)
      .map(ace => RowLevelSecurity(name = ace.role, grants = ace.grants.toSet)) ++
    tables.flatMap(
      _.rls
    )
  }
}

object Domain {
  def compare(existing: Domain, incoming: Domain): Try[DomainDiff] = {
    Try {
      val (addedTables, deletedTables, existingCommonTables) =
        AnyRefDiff.partitionNamed(existing.tables, incoming.tables)

      val commonTables: List[(Schema, Schema)] = existingCommonTables.map { table =>
        (
          table,
          incoming.tables
            .find(_.name.toLowerCase() == table.name.toLowerCase())
            .getOrElse(throw new Exception("Should not happen"))
        )
      }

      val updatedTablesDiff: List[SchemaDiff] = commonTables.flatMap { case (existing, incoming) =>
        Schema.compare(existing, incoming).toOption
      }

      val metadataDiff: ListDiff[Named] =
        AnyRefDiff.diffOptionAnyRef("metadata", existing.metadata, incoming.metadata)
      val tableRefsDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("tableRefs", existing.tableRefs.toSet, incoming.tableRefs.toSet)
      val commentDiff = AnyRefDiff.diffOptionString("comment", existing.comment, incoming.comment)
      val tagsDiffs = AnyRefDiff.diffSetString("tags", existing.tags, incoming.tags)
      val renameDiff = AnyRefDiff.diffOptionString("rename", existing.rename, incoming.rename)

      DomainDiff(
        name = existing.name,
        tables = SchemasDiff(
          added = addedTables.map(_.name),
          deleted = deletedTables.map(_.name),
          updated = updatedTablesDiff
        ),
        metadataDiff,
        tableRefsDiff,
        commentDiff,
        tagsDiffs,
        renameDiff
      )
    }
  }
}
