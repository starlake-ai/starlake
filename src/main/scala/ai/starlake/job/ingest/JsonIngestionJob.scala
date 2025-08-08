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
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import scala.util.Try

/** Main class to complex json delimiter separated values file If your json contains only one level
  * simple attribute aka. kind of dsv but in json format please use JSON_FLAT instead. It's way
  * faster
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
class JsonIngestionJob(
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

  protected def loadJsonData(): DataFrame = {
    val readOptions = (if (mergedMetadata.resolveArray()) {
                         // we force multiLine to true to keep current behavior where we allow only one array but we may disable it and allow multiple inline array as well, depending on user configuration
                         List("multiLine" -> "true", "lineSep" -> "\n")
                       } else
                         List("multiLine" -> mergedMetadata.resolveMultiline().toString)) ++ List(
      "encoding" -> mergedMetadata.resolveEncoding()
    ) ++ sparkOptions.toList
    val dfIn = session.read
      .options(readOptions.toMap)
      .json(path.map(_.toString): _*)
      .withColumn(
        //  Spark here can detect the input file automatically, so we're just using the input_file_name spark function
        CometColumns.cometInputFileNameColumn,
        org.apache.spark.sql.functions.input_file_name()
      )

    if (dfIn.columns.contains("_corrupt_record")) {
      // TODO send rejected records to rejected area
      dfIn.filter(col("_corrupt_record").isNotNull).show()
      logger.whenDebugEnabled {
        logger.debug(dfIn.filter(col("_corrupt_record").isNotNull).showString(1000, truncate = 0))
      }
      throw new Exception(
        s"""Invalid JSON File: ${path
            .map(_.toString)
            .mkString(",")}"""
      )
    } else {
      dfIn
    }
  }

  /** load the json as a dataframe of String
    *
    * @return
    *   Spark Dataframe loaded using metadata options
    */
  def loadDataSet(): Try[DataFrame] = {

    Try {
      val dfIn = loadJsonData()
      val dfInWithInputFilename = dfIn
        .withColumn(
          CometColumns.cometInputFileNameColumn,
          org.apache.spark.sql.functions
            .input_file_name()
        )
      logger.whenDebugEnabled {
        logger.debug(dfIn.schemaString())
      }
      dfInWithInputFilename
    }
  }

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] = {
    rejectedLines.write
      .format("json")
      .option("encoding", mergedMetadata.resolveEncoding())
      .options(sparkOptions)
  }
}
