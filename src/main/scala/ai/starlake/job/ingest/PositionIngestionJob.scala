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
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
  */
class PositionIngestionJob(
  domain: Domain,
  schema: Schema,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  options: Map[String, String]
)(implicit settings: Settings)
    extends DsvIngestionJob(domain, schema, types, path, storageHandler, schemaHandler, options) {

  /** Load dataset using spark csv reader and all metadata. Does not infer schema. columns not
    * defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return
    *   Spark DataFrame where each row holds a single string
    */
  override protected def loadDataSet(): Try[DataFrame] = {
    Try {
      val dfIn = metadata.getEncoding().toUpperCase match {
        case "UTF-8" => session.read.options(metadata.getOptions()).text(path.map(_.toString): _*)
        case _ => {
          val rdd = PositionIngestionUtil.loadDfWithEncoding(session, path, metadata.getEncoding())
          val schema: StructType = StructType(Array(StructField("value", StringType)))
          session.createDataFrame(rdd.map(line => Row.fromSeq(Seq(line))), schema)
        }
      }
      logger.whenDebugEnabled {
        logger.debug(dfIn.schemaString())
      }

      val df = applyIgnore(dfIn)

      metadata.withHeader match {
        case Some(true) =>
          throw new Exception("No Header allowed for Position File Format ")
        case Some(false) | None =>
          df
      }
    }
  }

  /** Apply the schema to the dataset. This is where all the magic happen Valid records are stored
    * in the accepted path / table and invalid records in the rejected path / table
    *
    * @param input
    *   : Spark Dataset
    */
  override protected def ingest(input: DataFrame): (Dataset[String], Dataset[Row]) = {

    val dataset: DataFrame =
      PositionIngestionUtil.prepare(session, input, schema.attributesWithoutScriptedFields)

    val orderedAttributes = reorderAttributes(dataset)

    val (orderedTypes, orderedSparkTypes) = reorderTypes(orderedAttributes)

    val validationResult = flatRowValidator.validate(
      session,
      metadata.getFormat(),
      metadata.getSeparator(),
      dataset,
      orderedAttributes,
      orderedTypes,
      orderedSparkTypes,
      settings.comet.privacy.options,
      settings.comet.cacheStorageLevel,
      settings.comet.sinkReplayToFile
    )
    saveRejected(validationResult.errors, validationResult.rejected)
    saveAccepted(validationResult)
    (validationResult.errors, validationResult.accepted)
  }

}

/** The Spark task that run on each worker
  */
object PositionIngestionUtil {

  def loadDfWithEncoding(session: SparkSession, path: List[Path], encoding: String) = {
    path
      .map(_.toString)
      .map(
        session.sparkContext
          .hadoopFile[LongWritable, Text, TextInputFormat](_)
          .map { case (_, content) => new String(content.getBytes, 0, content.getLength, encoding) }
      )
      .fold(session.sparkContext.emptyRDD)((r1, r2) => r1.union(r2))
  }

  def prepare(session: SparkSession, input: DataFrame, attributes: List[Attribute]) = {
    def getRow(inputLine: String, positions: List[Position]): Row = {
      val columnArray = new Array[String](positions.length)
      val inputLen = inputLine.length
      for (i <- positions.indices) {
        val first = positions(i).first
        val last = positions(i).last + 1
        columnArray(i) = if (last <= inputLen) inputLine.substring(first, last) else ""
      }
      Row.fromSeq(columnArray)
    }

    val positions = attributes.flatMap(_.position)
    val fieldTypeArray = new Array[StructField](positions.length)
    for (i <- attributes.indices) {
      fieldTypeArray(i) = StructField(s"col$i", StringType)
    }
    val rdd = input.rdd.map { row =>
      getRow(row.getString(0), positions)
    }

    val dataset =
      session.createDataFrame(rdd, StructType(fieldTypeArray)).toDF(attributes.map(_.name): _*)
    dataset withColumn (
      CometColumns.cometInputFileNameColumn,
      org.apache.spark.sql.functions.input_file_name()
    )
  }

}
