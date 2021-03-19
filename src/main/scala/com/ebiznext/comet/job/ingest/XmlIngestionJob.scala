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

package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.validator.TreeRowValidator
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil.compareTypes
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success, Try}

/** Main class to complex json delimiter separated values file
  * If your json contains only one level simple attribute aka. kind of dsv but in json format please use SIMPLE_JSON instead. It's way faster
  *
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
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
    * @return Spark Dataframe loaded using metadata options
    */
  protected def loadDataSet(): Try[DataFrame] = {
    try {
      val rowTag = metadata.xml.flatMap(_.get("rowTag"))
      rowTag.map { rowTag =>
        val df = path
          .map { singlePath =>
            session.read
              .format("com.databricks.spark.xml")
              .option("rowTag", rowTag)
              .option("inferSchema", value = false)
              .option("encoding", metadata.getEncoding())
              .load(singlePath.toString)
          }
          .reduce((acc, df) => acc union df)
        df.printSchema()
        Success(df)
      } getOrElse (Failure(
        throw new Exception(s"rowTag not found for schema ${domain.name}.${schema.name}")
      ))
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  lazy val schemaSparkType: StructType = schema.sparkType(schemaHandler)

  /** Where the magic happen
    *
    * @param dataset input dataset as a RDD of string
    */
  protected def ingest(dataset: DataFrame): (RDD[_], RDD[_]) = {
    dataset.printSchema()
    val datasetSchema = dataset.schema
    val errorList = compareTypes(schemaSparkType, datasetSchema)
    val rejectedRDD = session.sparkContext.parallelize(errorList)

    val withInputFileNameDS =
      dataset.withColumn(Settings.cometInputFileNameColumn, input_file_name())

    val appliedSchema = schema
      .sparkSchemaWithoutScriptedFields(schemaHandler)
      .add(StructField(Settings.cometInputFileNameColumn, StringType))
    val (koRDD, okRDD) =
      TreeRowValidator.validate(
        session,
        withInputFileNameDS,
        schema.attributes,
        types,
        appliedSchema
      )

    val allRejected = rejectedRDD.union(koRDD)
    saveRejected(allRejected)
    val transformedAcceptedDF = session.createDataFrame(okRDD, appliedSchema)
    saveAccepted(transformedAcceptedDF) // prefer to let Spark compute the final schema
    (allRejected, okRDD)
  }

  override def name: String = "JsonJob"
}

object XmlIngestionJob {

  def parseRDD(
    inputRDD: RDD[Row],
    schemaSparkType: StructType
  ): RDD[Either[List[String], Row]] = {
    inputRDD.mapPartitions { partition =>
      partition.map { row =>
        val rowSchema = row.schema
        val errorList = compareTypes(schemaSparkType, rowSchema)
        if (errorList.isEmpty)
          Right(row)
        else
          Left(errorList)
      }
    }
  }
}
