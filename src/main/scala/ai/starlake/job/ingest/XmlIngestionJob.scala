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

package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{DomainInfo, SchemaInfo, Type}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

import scala.util.Try

/** Main class to XML file If your json contains only one level simple attribute aka. kind of dsv
  * but in json format please use JSON_FLAT instead. It's way faster
  *
  * @param domain
  *   : Input Dataset Domain
  * @param schema
  *   : Input Dataset Schema
  * @param types
  *   : List of globally defined types
  * @param path
  *   : Input dataset path
  * @param storageHandler
  *   : Storage Handler
  */
class XmlIngestionJob(
  val domain: DomainInfo,
  val schema: SchemaInfo,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String],
  val accessToken: Option[String],
  val test: Boolean,
  val scheduledDate: Option[String]
)(implicit val settings: Settings)
    extends IngestionJob {

  /** load the json as a dataframe of String
    *
    * @return
    *   Spark Dataframe loaded using metadata options
    */
  def loadDataSet(): Try[DataFrame] = {
    val xmlOptions = mergedMetadata.getXmlOptions()
    Try {
      val rowTag = xmlOptions.get("rowTag")
      rowTag.map { _ =>
        val df = path
          .map { singlePath =>
            session.read
              .format("com.databricks.spark.xml")
              .options(xmlOptions)
              .option("inferSchema", value = false)
              .option("encoding", mergedMetadata.resolveEncoding())
              .options(sparkOptions)
              .schema(schema.sourceSparkSchemaUntypedEpochWithoutScriptedFields(schemaHandler))
              .load(singlePath.toString)
          }
          .reduce((acc, df) => acc union df)
        logger.whenInfoEnabled {
          logger.info(df.schemaString())
        }
        df
          .withColumn(
            CometColumns.cometInputFileNameColumn,
            org.apache.spark.sql.functions.input_file_name()
          )
      } getOrElse (
        throw new Exception(s"rowTag not found for schema ${domain.name}.${schema.name}")
      )
    }
  }

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] = {
    val xmlOptions = mergedMetadata.getXmlOptions()
    xmlOptions.get("rowTag") match {
      case Some(_) =>
        rejectedLines.write
          .format("com.databricks.spark.xml")
          .options(xmlOptions)
          .option("rootTag", xmlOptions.getOrElse("rootTag", schema.name))
          .option("encoding", mergedMetadata.resolveEncoding())
          .options(sparkOptions)
      case None =>
        throw new Exception(s"rowTag not found for schema ${domain.name}.${schema.name}")
    }
  }
}
