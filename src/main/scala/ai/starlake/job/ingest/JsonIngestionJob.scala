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
import ai.starlake.exceptions.NullValueFoundException
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{Domain, Schema, Type}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil.{compareTypes, parseString}
import org.apache.spark.sql.types.{DataType, StringType, StructField}

import scala.util.{Failure, Success, Try}

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

  protected def loadJsonData(): Dataset[String] = {
    if (mergedMetadata.resolveArray()) {
      session.read
        .option("multiLine", true)
        .json(path.map(_.toString): _*)
        .toJSON

    } else {
      session.read
        .option("inferSchema", value = false)
        .option("encoding", mergedMetadata.resolveEncoding())
        .options(sparkOptions)
        .textFile(path.map(_.toString): _*)
    }
  }

  /** load the json as an RDD of String
    *
    * @return
    *   Spark Dataframe loaded using metadata options
    */
  def loadDataSet(withSchema: Boolean): Try[DataFrame] = {

    Try {
      val dfIn = loadJsonData()
      val dfInWithInputFilename = dfIn.select(
        org.apache.spark.sql.functions
          .input_file_name()
          .alias(CometColumns.cometInputFileNameColumn),
        org.apache.spark.sql.functions.col("value")
      )
      logger.whenDebugEnabled {
        logger.debug(dfIn.schemaString())
      }
      val df = applyIgnore(dfInWithInputFilename)
      df
    }
  }

  private def validateRecord(record: String, schema: DataType): Array[String] = {
    parseString(record) match {
      case Success(datasetType) =>
        compareTypes(schema, datasetType).toArray
      case Failure(exception) =>
        Array(exception.toString)
    }
  }

  def parseDF(
    inputDF: DataFrame,
    schemaSparkType: DataType
  ): (Dataset[String], Dataset[String]) = {
    implicit val tuple3Encoder =
      Encoders.tuple(
        Encoders.STRING,
        Encoders.STRING,
        Encoders.STRING
      )
    val validatedDF = inputDF.map { row =>
      val inputFilename = row.getAs[String](CometColumns.cometInputFileNameColumn)
      val rowAsString = row.getAs[String]("value")
      val errorList = validateRecord(rowAsString, schemaSparkType)
      (rowAsString, errorList.mkString("\n"), inputFilename)
    }

    val withInvalidSchema =
      validatedDF
        .filter(_._2.nonEmpty)
        .map { case (_, errorList, _) =>
          errorList
        }(Encoders.STRING)

    val withValidSchema = validatedDF
      .filter(_._2.isEmpty)
      .map { case (rowAsString, _, inputFilename) =>
        val (left, _) = rowAsString.splitAt(rowAsString.lastIndexOf("}"))
        // Because Spark cannot detect the input files when session.read.json(session.createDataset(withValidSchema)(Encoders.STRING)),
        // We should add it as a normal field in the RDD before converting to a dataframe using session.read.json
        s"""$left, "${CometColumns.cometInputFileNameColumn}" : "$inputFilename" }"""
      }(Encoders.STRING)
    (withInvalidSchema, withValidSchema)
  }

  /** Where the magic happen
    *
    * @param dataset
    *   input dataset as a RDD of string
    */
  protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row], Long) = {
    val validationSchema = schema.sparkSchemaWithoutScriptedFieldsWithInputFileName(schemaHandler)
    val persistedDF = dataset.persist(settings.appConfig.cacheStorageLevel)
    val (withInvalidSchema, withValidSchema) = parseDF(persistedDF, validationSchema)

    val loadSchema = schema
      .sparkSchemaUntypedEpochWithoutScriptedFields(schemaHandler)
      .add(StructField(CometColumns.cometInputFileNameColumn, StringType))

    val toValidate = session.read
      .schema(loadSchema)
      .json(withValidSchema)

    val validationResult =
      treeRowValidator.validate(
        session,
        mergedMetadata.resolveFormat(),
        mergedMetadata.resolveSeparator(),
        toValidate,
        schema.attributes,
        types,
        validationSchema,
        settings.appConfig.privacy.options,
        settings.appConfig.cacheStorageLevel,
        settings.appConfig.sinkReplayToFile,
        mergedMetadata.emptyIsNull.getOrElse(settings.appConfig.emptyIsNull)
      )
    val rejectedDS = withInvalidSchema.union(validationResult.errors)
    rejectedDS.persist(settings.appConfig.cacheStorageLevel)

    saveRejected(rejectedDS, validationResult.rejected)(
      settings,
      storageHandler,
      schemaHandler
    ).flatMap { _ =>
      saveAccepted(validationResult) // prefer to let Spark compute the final schema
    } match {
      case Failure(exception: NullValueFoundException) =>
        (validationResult.errors.union(rejectedDS), validationResult.accepted, exception.nbRecord)
      case Failure(exception) =>
        throw exception
      case Success(rejectedRecordCount) =>
        (validationResult.errors.union(rejectedDS), validationResult.accepted, rejectedRecordCount);
    }
  }
}
