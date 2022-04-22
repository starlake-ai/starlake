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
import ai.starlake.schema.model.{Domain, Schema, Type}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

/** Main class to complex json delimiter separated values file If your json contains only one level
  * simple attribute aka. kind of dsv but in json format please use SIMPLE_JSON instead. It's way
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
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String]
)(implicit val settings: Settings)
    extends IngestionJob {

  protected def loadJsonData(): Dataset[String] = {
    if (metadata.isArray()) {
      val jsonRDD =
        session.sparkContext.wholeTextFiles(path.map(_.toString).mkString(",")).map {
          case (_, content) => content
        }
      session.read.json(session.createDataset(jsonRDD)(Encoders.STRING)).toJSON

    } else {
      session.read
        .option("inferSchema", value = false)
        .option("encoding", metadata.getEncoding())
        .options(metadata.getOptions())
        .textFile(path.map(_.toString): _*)
    }

  }

  /** load the json as an RDD of String
    *
    * @return
    *   Spark Dataframe loaded using metadata options
    */
  protected def loadDataSet(): Try[DataFrame] = {

    Try {
      val dfIn = loadJsonData()
      val dfInWithInputFilename = dfIn.select(
        org.apache.spark.sql.functions.input_file_name(),
        org.apache.spark.sql.functions.col("value")
      )
      logger.whenDebugEnabled {
        logger.debug(dfIn.schemaString())
      }
      val df = applyIgnore(dfInWithInputFilename)
      df
    }
  }

  lazy val schemaSparkType: StructType = schema.sourceSparkSchema(schemaHandler)

  /** Where the magic happen
    *
    * @param dataset
    *   input dataset as a RDD of string
    */
  protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row]) = {
    val rdd: RDD[Row] = dataset.rdd

    val parsed: RDD[Either[List[String], (String, String)]] = JsonIngestionUtil
      .parseRDD(rdd, schemaSparkType)
      .persist(settings.comet.cacheStorageLevel)

    val withValidSchema: RDD[String] =
      parsed
        .collect { case Right(value) =>
          value
        }
        .map { case (row, inputFileName) =>
          val (left, _) = row.splitAt(row.lastIndexOf("}"))

          // Because Spark cannot detect the input files when session.read.json(session.createDataset(withValidSchema)(Encoders.STRING)),
          // We should add it as a normal field in the RDD before converting to a dataframe using session.read.json

          s"""$left, "${CometColumns.cometInputFileNameColumn}" : "$inputFileName" }"""
        }

    val withInvalidSchema: RDD[String] =
      parsed
        .collect { case Left(value) =>
          value
        }
        .map(_.mkString("\n"))

    val loadSchema = schema
      .sparkSchemaUntypedEpochWithoutScriptedFields(schemaHandler)
      .add(StructField(CometColumns.cometInputFileNameColumn, StringType))

    val validationSchema = schema.sparkSchemaWithoutScriptedFieldsWithInputFileName(schemaHandler)

    val toValidate = session.read
      .schema(loadSchema)
      .json(session.createDataset(withValidSchema)(Encoders.STRING))

    val validationResult =
      treeRowValidator.validate(
        session,
        metadata.getFormat(),
        metadata.getSeparator(),
        toValidate,
        schema.attributes,
        types,
        validationSchema,
        settings.comet.privacy.options,
        settings.comet.cacheStorageLevel,
        settings.comet.sinkReplayToFile
      )

    import session.implicits._
    val rejectedDS = session.createDataset(withInvalidSchema).union(validationResult.errors)

    saveRejected(rejectedDS, validationResult.rejected)
    saveAccepted(validationResult) // prefer to let Spark compute the final schema
    (rejectedDS, toValidate)
  }

  override def name: String = "JsonJob"
}
