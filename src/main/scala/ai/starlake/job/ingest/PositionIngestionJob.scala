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
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
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
  options: Map[String, String],
  accessToken: Option[String],
  test: Boolean
)(implicit settings: Settings)
    extends DsvIngestionJob(
      domain,
      schema,
      types,
      path,
      storageHandler,
      schemaHandler,
      options,
      accessToken,
      test
    ) {

  /** Load dataset using spark csv reader and all metadata. Does not infer schema. columns not
    * defined in the schema are dropped from the dataset (require datsets with a header)
    *
    * @return
    *   Spark DataFrame where each row holds a single string
    */
  override def loadDataSet(): Try[DataFrame] = {
    Try {
      val dfIn = mergedMetadata.resolveEncoding().toUpperCase match {
        case "UTF-8" =>
          session.read.options(sparkOptions).text(path.map(_.toString): _*)
        case _ =>
          PositionIngestionUtil.loadDfWithEncoding(
            session,
            path,
            mergedMetadata.resolveEncoding()
          )
      }
      logger.whenDebugEnabled {
        logger.debug(dfIn.schemaString())
      }
      val preparedDF =
        PositionIngestionUtil.prepare(session, dfIn, schema.attributesWithoutScriptedFields)
      preparedDF
        .withColumn(
          CometColumns.cometInputFileNameColumn,
          org.apache.spark.sql.functions.input_file_name()
        )
    }
  }

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] = {
    rejectedLines
      .select(
        functions
          .concat(
            schema.attributesWithoutScriptedFields.map(attr => col(attr.name).cast(StringType)): _*
          )
          .as("input_line")
      )
      .write
      .format("csv")
      .option("encoding", mergedMetadata.resolveEncoding())
      .option("header", false)
      .option("delimiter", "\n")
      .option("quote", "\n")
  }

}

/** The Spark task that run on each worker
  */
object PositionIngestionUtil {

  def loadDfWithEncoding(
    session: SparkSession,
    path: List[Path],
    encoding: String
  ): DataFrame = {
    // This is an hack of how to use CSV source as a text source and have the ability to specify an encoding.
    // Using wholeTextFile requires the input to be in UTF-8
    session.read
      .schema(StructType(Array(StructField("value", StringType))))
      .option("encoding", encoding)
      .option("delimiter", "\n")
      .csv(path.map(_.toString): _*)
  }

  def prepare(session: SparkSession, input: DataFrame, attributes: List[Attribute]): DataFrame = {
    import org.apache.spark.sql.functions._
    val attributesProjection = attributes.foldLeft(List[Column]()) { (projection, attribute) =>
      val attributePosition = attribute.position.getOrElse(
        throw new RuntimeException(s"Attribute ${attribute.name} does not have position set")
      )
      // spark substring is 1-based and last is the length in spark and not the position to exclude
      projection :+ substring(
        col("value"),
        attributePosition.first + 1,
        attributePosition.last - attributePosition.first + 1
      ).as(attribute.name)
    }

    input
      .select(attributesProjection: _*)
      .withColumn(
        CometColumns.cometInputFileNameColumn,
        org.apache.spark.sql.functions.input_file_name()
      )
  }
}
