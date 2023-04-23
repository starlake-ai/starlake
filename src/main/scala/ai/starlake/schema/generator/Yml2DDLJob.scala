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

package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.extract.{JDBCSchema, JDBCUtils}
import ai.starlake.extract.JDBCUtils.{Columns, PrimaryKeys, TableRemarks}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Domain, Schema}
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{Path => StoragePath}
import org.fusesource.scalate.{TemplateEngine, TemplateSource}

import scala.util.Try

/** * Infers the schema of a given data path, domain name, schema name.
  */
class Yml2DDLJob(config: Yml2DDLConfig, schemaHandler: SchemaHandler)(implicit
  settings: Settings
) extends StrictLogging {
  implicit class CaseInsensitiveGetMap[V](m: Map[String, V]) {
    def iget(key: String): Option[V] = m
      .get(key.toLowerCase())
      .orElse(m.get(key.toUpperCase())) // you can add more orElse in chain
  }
  val engine: TemplateEngine = new TemplateEngine

  def name: String = "InferDDL"

  /** Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  def run(): Try[Unit] =
    Try {
      val domains = config.domain match {
        case None => schemaHandler.domains()
        case Some(domain) =>
          val res = schemaHandler
            .domains()
            .find(_.name.toLowerCase() == domain)
            .getOrElse(throw new Exception(s"Domain $domain not found"))
          List(res)
      }
      val sqlString = new StringBuffer()
      Try(Domain.ddlMapping(config.datawarehouse, "global")) match {
        case scala.util.Success(_) =>
          // global.ssp exists
          val customParamMap = Map(
            "domains" -> domains
          )
          val result = applyTemplate("global", customParamMap)
          sqlString.append(result)
        case scala.util.Failure(_) =>
        // no global.ssp file, do nothing
      }

      domains.map { domain =>
        val domainLabels = Utils.labels(domain.tags)

        Try(Domain.ddlMapping(config.datawarehouse, "domain")) match {
          case scala.util.Success(_) =>
            // domain.ssp exists
            val customParamMap = Map(
              "domain"        -> domain,
              "domainName"    -> domain.finalName,
              "domainComment" -> domain.comment.getOrElse(""),
              "domainLabels"  -> domainLabels
            )
            val result = applyTemplate("domain", customParamMap)
            sqlString.append(result)
          case scala.util.Failure(_) =>
          // no domain.ssp file, do nothing
        }
        val schemas: Seq[Schema] = config.schemas match {
          case Some(schemas) =>
            schemas.flatMap(schema =>
              domain.tables.find(_.name.toLowerCase() == schema.toLowerCase)
            )
          case None =>
            domain.tables
        }
        val existingTables = config.connection match {
          case Some(connection) =>
            val connectionOptions = settings.comet.connections(connection).options
            JDBCUtils.extractJDBCTables(
              JDBCSchema(
                config.catalog,
                domain.finalName,
                None,
                None,
                Nil,
                List("TABLE"),
                None
              ),
              connectionOptions,
              skipRemarks = true
            )
          case None => Map.empty[String, (TableRemarks, Columns, PrimaryKeys)]
        }
        val toDeleteTables = existingTables.keys.filterNot(table =>
          schemas.map(_.finalName.toLowerCase()).contains(table.toLowerCase())
        )

        toDeleteTables.foreach { table =>
          val schema: String = schemas
            .collectFirst {
              case s if s.finalName.toLowerCase() == table.toLowerCase() => s.finalName
            }
            .getOrElse(table)

          val dropParamMap = Map(
            "attributes"              -> List.empty[Map[String, Any]],
            "newAttributes"           -> Nil,
            "alterAttributes"         -> Nil,
            "alterCommentAttributes"  -> Nil,
            "alterDataTypeAttributes" -> Nil,
            "alterRequiredAttributes" -> Nil,
            "droppedAttributes"       -> Nil,
            "domainName"              -> domain.finalName,
            "tableName"               -> schema,
            "partitions"              -> Nil,
            "clustered"               -> Nil,
            "primaryKeys"             -> Nil,
            "comment"                 -> "",
            "domainComment"           -> ""
          )
          logger.info(s"Dropping table $table")
          val result = applyTemplate("drop", dropParamMap)
          sqlString.append(result)
        }

        schemas.map { schema =>
          val schemaLabels = Utils.labels(schema.tags)

          val ddlFields = schema.ddlMapping(config.datawarehouse, schemaHandler)

          val mergedMetadata = schema.mergedMetadata(domain.metadata)

          val isNew = existingTables.iget(schema.finalName).isEmpty
          if (isNew) {
            val createParamMap = Map(
              "attributes"              -> ddlFields.map(_.toMap()),
              "newAttributes"           -> Nil,
              "alterAttributes"         -> Nil,
              "alterCommentAttributes"  -> Nil,
              "alterDataTypeAttributes" -> Nil,
              "alterRequiredAttributes" -> Nil,
              "droppedAttributes"       -> Nil,
              "domainName"              -> domain.finalName,
              "tableName"               -> schema.finalName,
              "domain"                  -> domain,
              "table"                   -> schema,
              "partitions"    -> mergedMetadata.partition.map(_.getAttributes()).getOrElse(Nil),
              "clustered"     -> mergedMetadata.clustering.getOrElse(Nil),
              "primaryKeys"   -> schema.primaryKey,
              "tableComment"  -> schema.comment.getOrElse(""),
              "domainComment" -> domain.comment.getOrElse(""),
              "domainLabels"  -> domainLabels,
              "tableLabels"   -> schemaLabels,
              "sink"          -> mergedMetadata.sink,
              "metadata"      -> mergedMetadata
            )
            println(s"Creating new table ${schema.finalName}")
            val result = applyTemplate("create", createParamMap)
            sqlString.append(result)
          } else {
            val (_, existingColumns, _) =
              existingTables
                .iget(schema.finalName)
                .getOrElse(throw new Exception("Should never happen"))
            val addColumns =
              schema.attributes.filter(attr =>
                !existingColumns
                  .map(_.getFinalName().toLowerCase())
                  .contains(attr.getFinalName().toLowerCase())
              )
            val dropColumns =
              existingColumns.filter(attr =>
                !schema.attributes
                  .map(_.getFinalName().toLowerCase())
                  .contains(attr.getFinalName().toLowerCase())
              )
            val alterColumns =
              schema.attributes.filter { attr =>
                existingColumns.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  (existingAttr.required != attr.required ||
                  !existingAttr.samePrimitiveType(attr)(schemaHandler) ||
                  existingAttr.comment.getOrElse("") != attr.comment.getOrElse(""))
                )
              }
            val alterDataTypeColumns =
              schema.attributes.filter { attr =>
                existingColumns.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  !existingAttr.samePrimitiveType(attr)(schemaHandler)
                )
              }
            val alterDescriptionColumns =
              schema.attributes.filter { attr =>
                existingColumns.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  existingAttr.comment.getOrElse("") != attr.comment.getOrElse("")
                )
              }
            val alterRequiredColumns =
              schema.attributes.filter { attr =>
                existingColumns.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  existingAttr.required != attr.required
                )
              }

            val alterParamMap = Map(
              "attributes" -> Nil,
              "newAttributes" -> addColumns.map(
                _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
              ),
              "alterAttributes" -> alterColumns.map(
                _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
              ),
              "alterCommentAttributes" -> alterDescriptionColumns.map(
                _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
              ),
              "alterDataTypeAttributes" -> alterDataTypeColumns.map(
                _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
              ),
              "alterRequiredAttributes" -> alterRequiredColumns.map(
                _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
              ),
              "droppedAttributes" -> dropColumns.map(
                _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
              ),
              "domainName"    -> domain.finalName,
              "tableName"     -> schema.finalName,
              "domain"        -> domain,
              "table"         -> schema,
              "partitions"    -> Nil,
              "clustered"     -> Nil,
              "primaryKeys"   -> Nil,
              "tableComment"  -> schema.comment.getOrElse(""),
              "domainComment" -> domain.comment.getOrElse(""),
              "domainLabels"  -> domainLabels,
              "tableLabels"   -> schemaLabels
            )
            val result = applyTemplate("alter", alterParamMap)
            println(s"Altering existing table ${schema.finalName}")
            sqlString.append(result)
          }
        }
      }

      val sqlScript = sqlString.toString
      logger.debug(s"Final script is:\n $sqlScript")

      val outputPath =
        File(
          config.outputPath.getOrElse(settings.comet.metadata),
          "ddl",
          config.datawarehouse + ".sql"
        )
      writeScript(sqlScript, outputPath.pathAsString).toOption

      if (config.apply)
        config.connection.fold(logger.warn("Could not apply script, connection is not defined"))(
          conn => JDBCUtils.applyScript(sqlScript, settings.comet.connections(conn).options)
        )
    }

  private def applyTemplate(
    ddlType: TableRemarks,
    dropParamMap: Map[TableRemarks, Any]
  ): String = {
    val (templatePath, templateContent) =
      Domain.ddlMapping(
        config.datawarehouse,
        ddlType
      )
    engine.layout(
      TemplateSource.fromText(templatePath.toString, templateContent),
      dropParamMap
    )
  }
  private def writeScript(sqlScript: String, output: String): Try[Unit] = {
    Try(settings.storageHandler.write(sqlScript, new StoragePath(output)))
  }
}
