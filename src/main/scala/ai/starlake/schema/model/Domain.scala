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
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path
import ai.starlake.schema.model.Severity._

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
  tables: List[Schema] = Nil, // deprecated("Moved to tableRefs", "0.6.4")
  comment: Option[String] = None,
  @nowarn @deprecated("Moved to Metadata", "0.2.8") ack: Option[String] = None,
  tags: Set[String] = Set.empty,
  rename: Option[String] = None,
  database: Option[String] = None
) extends Named {

  def this() = this("") // Should never be called. Here for Jackson deserialization only

  /** @return
    *   renamed column if defined, source name otherwise
    */
  @JsonIgnore
  lazy val finalName: String = rename.getOrElse(name)

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
    if (settings.storageHandler().exists(template))
      Some(settings.storageHandler().read(template))
    else
      None
  }

  /** Resolve method are here to handle backward compatibility
    * @return
    */
  @nowarn def resolveDirectory(): String = {
    resolveDirectoryOpt() match {
      case Some(directory) => directory
      case None =>
        throw new Exception(
          """directory attribute is mandatory for the "import" command. should never happen"""
        )
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
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {

    val messageList: mutable.MutableList[ValidationMessage] = mutable.MutableList.empty

    // Check Domain name validity
    val forceDomainPrefixRegex = settings.comet.forceDomainPattern.r
    // TODO: name doesn't need to respect the pattern because it may be renamed. Restriction is based on target database restriction.
    // We may check depending on the sink type but we may sink differently for each table.
    // It would be better to assume a starlake pattern to describe a dataset and the container of the dataset such as the bigquery syntax project:dataset
    // and then apply this syntax to all databases even if natively they don't accept that.
    // Therefore, it means that we need to adapt on writing to the database, the target name.
    // The same applies to table name.
    if (!forceDomainPrefixRegex.pattern.matcher(name).matches())
      messageList += ValidationMessage(
        Error,
        "Domain",
        s"name: Domain with name $name should respect the pattern ${forceDomainPrefixRegex.regex}"
      )

    val forceTablePrefixRegex = settings.comet.forceTablePattern.r

    // Check Schemas validity
    tables.foreach { schema =>
      for (errors <- schema.checkValidity(this.metadata, schemaHandler).left) {
        messageList ++= errors
      }
    }

    // Check Metadata validity
    metadata.foreach { metadata =>
      for (errors <- metadata.checkValidity(schemaHandler).left) {
        messageList ++= errors
      }
    }

    val duplicatesErrorMessage =
      "Schema %s defined %d times. A schema can only be defined once."
    for (
      errors <- Utils.duplicates("Table name", tables.map(_.name), duplicatesErrorMessage).left
    ) {
      messageList ++= errors
    }

    // TODO Check partition columns
    if (messageList.nonEmpty)
      Left(messageList.toList)
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
      .map(t => (t.finalName, t.rls))
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
  def checkFilenamesValidity()(implicit
    storage: StorageHandler,
    settings: Settings
  ): List[Either[List[ValidationMessage], Boolean]] = {
    val domainRootFiles = storage.list(DatasetArea.load, recursive = false, exclude = None)
    val domainRootDirectories = storage.listDirectories(DatasetArea.load)
    val diff = domainRootFiles.diff(domainRootDirectories)
    val extraFileWarnings = if (diff.nonEmpty) {
      List(
        Left(
          List(
            ValidationMessage(
              Warning,
              "Domain",
              s"Domain root directory should only contain directories. Found ${diff.mkString(",")}"
            )
          )
        )
      )
    } else {
      List(Right(true))
    }
    val allWarnings = domainRootDirectories.flatMap { domainRootDirectory =>
      val domainName = domainRootDirectory.getName()
      val domainDirectory = new Path(domainRootDirectory, domainName)
      val expectedDomainYmlName = s"_config.comet.yml"
      val expectedDomainYmlPath = new Path(domainDirectory, expectedDomainYmlName)
      val domainYmlExists = storage.exists(expectedDomainYmlPath)
      val domainYmlWarnings = if (domainYmlExists) {
        Nil
      } else {
        List(
          Left(
            List(
              ValidationMessage(
                Warning,
                "Domain",
                s"Domain directory for $domainName should contain a _config.comet.yml file"
              )
            )
          )
        )
      }
      List(extraFileWarnings, domainYmlWarnings).flatten
    }
    allWarnings
  }

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
        commentDiff,
        tagsDiffs,
        renameDiff
      )
    }
  }

  def ddlExtract(datawarehouse: String, ddlType: String)(implicit
    settings: Settings
  ): (Path, String) = {
    val rootPath = new Path(new Path(DatasetArea.extract, "ddl"), datawarehouse)
    val mustache = new Path(rootPath, s"$ddlType.mustache")
    val ssp = new Path(rootPath, s"$ddlType.ssp")
    val template =
      if (settings.storageHandler().exists(mustache))
        mustache
      else if (settings.storageHandler().exists(ssp))
        ssp
      else
        throw new Exception(s"No $ddlType.mustache/ssp found for datawarehouse $datawarehouse")
    template -> settings.storageHandler().read(template)
  }

}
