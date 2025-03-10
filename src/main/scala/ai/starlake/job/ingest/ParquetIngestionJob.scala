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

import ai.starlake.exceptions.NullValueFoundException
import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

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
class ParquetIngestionJob(
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String],
  val accessToken: Option[String],
  val test: Boolean
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
      val format = mergedMetadata.getOptions().getOrElse("format", "parquet")
      val dfIn = session.read
        .options(sparkOptions)
        .format(format)
        .load(path.map(_.toString): _*)

      logger.debug(dfIn.schema.treeString)
      if (dfIn.isEmpty) {
        // empty dataframe with accepted schema
        val sparkSchema = schema.sourceSparkSchemaWithoutScriptedFields(schemaHandler)

        session
          .createDataFrame(rows = new java.util.ArrayList(), StructType(sparkSchema))
          .withColumn(
            CometColumns.cometInputFileNameColumn,
            org.apache.spark.sql.functions.input_file_name()
          )
      } else {
        val datasetHeaders = dfIn.columns.toList
        val (_, drop) = intersectHeaders(datasetHeaders, schemaHeaders)
        if (datasetHeaders.length == drop.length) {
          throw new Exception(s"""No attribute found in input dataset ${path.toString}
                                 | SchemaHeaders : ${schemaHeaders.mkString(",")}
                                 | Dataset Headers : ${datasetHeaders.mkString(",")}
             """.stripMargin)
        }
        val resDF = dfIn.drop(drop: _*)
        resDF.withColumn(
          //  Spark here can detect the input file automatically, so we're just using the input_file_name spark function
          CometColumns.cometInputFileNameColumn,
          org.apache.spark.sql.functions.input_file_name()
        )
      }
    }
  }

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] = {
    val format = mergedMetadata.getOptions().getOrElse("format", "parquet")
    rejectedLines.write.options(sparkOptions).format(format)
  }
}
