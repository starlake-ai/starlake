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
import ai.starlake.schema.model.*
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.util.Try

/** Main class to ingest delimiter separated values file
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
  * @param options
  *   : Parameters to pass as input (k1=v1,k2=v2,k3=v3)
  */
class DsvIngestionJob(
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

  /** dataset Header names as defined by the schema
    */
  val schemaHeaders: List[String] = schema.attributes.map(_.name)

  /** Load dataset using spark csv reader and all metadata. Does not infer schema. columns not
    * defined in the schema are dropped from the dataset (require datsets with a header)
    *
    * @return
    *   Spark Dataset
    */
  def loadDataSet(): Try[DataFrame] = {
    Try {
      val dfInReader = session.read
        .option("header", mergedMetadata.resolveWithHeader().toString)
        .option("inferSchema", value = false)
        .option("delimiter", mergedMetadata.resolveSeparator())
        .option("multiLine", mergedMetadata.resolveMultiline())
        .option("quote", mergedMetadata.resolveQuote())
        .option("escape", mergedMetadata.resolveEscape())
        .option("nullValue", mergedMetadata.resolveNullValue())
        .option("parserLib", "UNIVOCITY")
        .option("encoding", mergedMetadata.resolveEncoding())
        .options(sparkOptions)
        .options(settings.appConfig.dsvOptions)
      val finalDfInReader =
        if (mergedMetadata.resolveWithHeader()) {
          dfInReader
        } else {
          // In a DSV file there is no depth so we can just traverse the first level
          val inputSchema = TableAttribute(
            name = "root",
            `type` = PrimitiveType.struct.value,
            attributes = schema.attributesWithoutScriptedFields
          ).sparkType(
            schemaHandler,
            (_, sf) => sf.copy(dataType = StringType, nullable = true)
          ) match {
            case st: StructType => st
            case _ =>
              throw new RuntimeException(
                "Should never happen since we just converted root of type struct to spark type."
              )
          }
          dfInReader.schema(inputSchema)
        }

      val dfIn = finalDfInReader.csv(path.map(_.toString): _*)

      logger.debug(dfIn.schema.treeString)

      val dfInSkipped =
        options.get("skipRows") match {
          case Some(value) =>
            val dfWithIndex =
              dfIn.withColumn(
                "sl_monotonically_increasing_id",
                row_number().over(Window.orderBy(monotonically_increasing_id()))
              )
            // Filter and clean up
            val dfSkipped = dfWithIndex
              .filter(col("sl_monotonically_increasing_id") > value.toInt)
              .drop("sl_monotonically_increasing_id")
            dfSkipped
          case None =>
            dfIn
        }

      dfInSkipped
        .withColumn(
          CometColumns.cometInputFileNameColumn,
          org.apache.spark.sql.functions.input_file_name()
        )
    }
  }

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] = {
    rejectedLines.write
      .format("csv")
      .option("header", mergedMetadata.resolveWithHeader().toString)
      .option("delimiter", mergedMetadata.resolveSeparator())
      .option("quote", mergedMetadata.resolveQuote())
      .option("escape", mergedMetadata.resolveEscape())
      .option("nullValue", mergedMetadata.resolveNullValue())
      .option("parserLib", "UNIVOCITY")
      .option("encoding", mergedMetadata.resolveEncoding())
      .options(sparkOptions)
      .options(settings.appConfig.dsvOptions)
  }
}
