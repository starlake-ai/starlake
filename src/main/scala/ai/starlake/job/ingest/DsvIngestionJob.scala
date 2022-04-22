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
import org.apache.spark.sql.types.StructType

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
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String]
)(implicit val settings: Settings)
    extends IngestionJob {

  /** @return
    *   Spark Job name
    */
  override def name: String =
    s"""${domain.name}-${schema.name}-${path.headOption.map(_.getName).mkString(",")}"""

  /** dataset Header names as defined by the schema
    */
  val schemaHeaders: List[String] = schema.attributes.map(_.name)

  /** remove any extra quote / BOM in the header
    *
    * @param header
    *   : Header column name
    * @return
    */
  private def cleanHeaderCol(header: String): String =
    header.replaceAll("\"", "").replaceAll("\uFEFF", "")

  /** @param datasetHeaders
    *   : Headers found in the dataset
    * @param schemaHeaders
    *   : Headers defined in the schema
    * @return
    *   success if all headers in the schema exist in the dataset
    */
  private def validateHeader(datasetHeaders: List[String], schemaHeaders: List[String]): Boolean = {
    schemaHeaders.forall(schemaHeader => datasetHeaders.contains(schemaHeader))
  }

  /** Load dataset using spark csv reader and all metadata. Does not infer schema. columns not
    * defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return
    *   Spark Dataset
    */
  protected def loadDataSet(): Try[DataFrame] = {
    Try {
      val dfIn = session.read
        .option("header", metadata.isWithHeader().toString)
        .option("inferSchema", value = false)
        .option("delimiter", metadata.getSeparator())
        .option("multiLine", metadata.getMultiline())
        .option("quote", metadata.getQuote())
        .option("escape", metadata.getEscape())
        .option("parserLib", "UNIVOCITY")
        .option("encoding", metadata.getEncoding())
        .options(metadata.getOptions())
        .options(settings.comet.dsvOptions)
        .csv(path.map(_.toString): _*)

      logger.debug(dfIn.schema.treeString)
      if (dfIn.isEmpty) {
        // empty dataframe with accepted schema
        val sparkSchema = schema.sparkSchemaWithoutScriptedFields(schemaHandler)

        session
          .createDataFrame(session.sparkContext.emptyRDD[Row], StructType(sparkSchema))
          .withColumn(
            CometColumns.cometInputFileNameColumn,
            org.apache.spark.sql.functions.input_file_name()
          )
      } else {
        val df = applyIgnore(dfIn)

        val resDF = metadata.withHeader match {
          case Some(true) =>
            val datasetHeaders: List[String] = df.columns.toList.map(cleanHeaderCol)
            val (_, drop) = intersectHeaders(datasetHeaders, schemaHeaders)
            if (datasetHeaders.length == drop.length) {
              throw new Exception(s"""No attribute found in input dataset ${path.toString}
                                   | SchemaHeaders : ${schemaHeaders.mkString(",")}
                                   | Dataset Headers : ${datasetHeaders.mkString(",")}
             """.stripMargin)
            }
            df.drop(drop: _*)
          case Some(false) | None =>
            /* No header, let's make sure we take the first attributes
             if there are more in the CSV file
             */
            val attributesWithoutScriptedFields = schema.attributesWithoutScriptedFields
            val compare =
              attributesWithoutScriptedFields.length.compareTo(df.columns.length)
            compare match {
              case 0 =>
                df.toDF(
                  attributesWithoutScriptedFields
                    .map(_.name)
                    .take(attributesWithoutScriptedFields.length): _*
                )
              case c if c > 0 =>
                val countMissing =
                  attributesWithoutScriptedFields.length - df.columns.length
                throw new Exception(s"$countMissing MISSING columns in the input DataFrame ")
              case _ => // compare < 0
                val cols = df.columns
                df.select(
                  cols.head,
                  cols.tail.take(attributesWithoutScriptedFields.length - 1): _*
                ).toDF(attributesWithoutScriptedFields.map(_.name): _*)
            }
        }
        resDF.withColumn(
          //  Spark here can detect the input file automatically, so we're just using the input_file_name spark function
          CometColumns.cometInputFileNameColumn,
          org.apache.spark.sql.functions.input_file_name()
        )
      }
    }
  }

  /** Apply the schema to the dataset. This is where all the magic happen Valid records are stored
    * in the accepted path / table and invalid records in the rejected path / table
    *
    * @param dataset
    *   : Spark Dataset
    */
  protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row]) = {

    val orderedAttributes = reorderAttributes(dataset)

    def reorderTypes(): (List[Type], StructType) = {
      val typeMap: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
      val (tpes, sparkFields) = orderedAttributes.map { attribute =>
        val tpe = typeMap(attribute.`type`)
        (tpe, tpe.sparkType(attribute.name, !attribute.required, attribute.comment))
      }.unzip
      (tpes, StructType(sparkFields))
    }

    val (orderedTypes, orderedSparkTypes) = reorderTypes()

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
