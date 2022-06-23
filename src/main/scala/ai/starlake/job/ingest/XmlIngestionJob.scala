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
import ai.starlake.job.validator.ValidationResult
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{Domain, Schema, Type}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil.compareTypes
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.Try

/** Main class to XML file If your json contains only one level simple attribute aka. kind of dsv
  * but in json format please use SIMPLE_JSON instead. It's way faster
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
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String]
)(implicit val settings: Settings)
    extends IngestionJob {

  /** load the json as an RDD of String
    *
    * @return
    *   Spark Dataframe loaded using metadata options
    */
  protected def loadDataSet(): Try[DataFrame] = {
    val xmlOptions = metadata.getXmlOptions()
    Try {
      val rowTag = xmlOptions.get("rowTag")
      rowTag.map { rowTag =>
        val df = path
          .map { singlePath =>
            session.read
              .format("com.databricks.spark.xml")
              .options(xmlOptions)
              .option("inferSchema", value = false)
              .option("encoding", metadata.getEncoding())
              .options(metadata.getOptions())
              .schema(schema.sparkSchemaUntypedEpochWithoutScriptedFields(schemaHandler))
              .load(singlePath.toString)
          }
          .reduce((acc, df) => acc union df)
        logger.whenInfoEnabled {
          logger.info(df.schemaString())
        }
        df
      } getOrElse (
        throw new Exception(s"rowTag not found for schema ${domain.name}.${schema.name}")
      )
    }
  }

  lazy val schemaSparkType: StructType = schema.sourceSparkSchema(schemaHandler)

  /** Where the magic happen
    *
    * @param dataset
    *   input dataset as a RDD of string
    */
  protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row]) = {
    import session.implicits._
    val datasetSchema = dataset.schema
    val errorList = compareTypes(schemaSparkType, datasetSchema)
    val rejectedDS = errorList.toDS
    metadata.xml.flatMap(_.get("skipValidation")) match {
      case Some(_) =>
        val rejectedDS = errorList.toDS()
        saveRejected(rejectedDS, session.emptyDataset[String])
        saveAccepted(
          ValidationResult(
            session.emptyDataset[String],
            session.emptyDataset[String],
            dataset
          )
        )
        (rejectedDS, dataset)
      case None =>
        val withInputFileNameDS =
          dataset.withColumn(CometColumns.cometInputFileNameColumn, input_file_name())

        val validationSchema =
          schema.sparkSchemaWithoutScriptedFieldsWithInputFileName(schemaHandler)

        val validationResult =
          treeRowValidator.validate(
            session,
            metadata.getFormat(),
            metadata.getSeparator(),
            withInputFileNameDS,
            schema.attributes,
            types,
            validationSchema,
            settings.comet.privacy.options,
            settings.comet.cacheStorageLevel,
            settings.comet.sinkReplayToFile
          )

        val allRejected = rejectedDS.union(validationResult.errors)
        saveRejected(allRejected, validationResult.rejected)
        saveAccepted(validationResult)
        (allRejected, validationResult.accepted)
    }
  }

  override def name: String =
    s"""XML-${domain.name}-${schema.name}-${path.headOption.map(_.getName).mkString(",")}"""
}
