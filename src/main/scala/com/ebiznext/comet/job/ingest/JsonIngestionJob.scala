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
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

import scala.collection.mutable
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
        session.sparkContext.wholeTextFiles(path.map(_.toString).mkString(",")).map(_._2)
      import org.apache.spark.sql._
      session.read.json(session.createDataset(jsonRDD)(Encoders.STRING)).toJSON

    } else {
      session.read
        .option("inferSchema", value = false)
        .option("encoding", metadata.getEncoding())
        .textFile(path.map(_.toString): _*)
    }

  }

  /** load the json as an RDD of String
    *
    * @return Spark Dataframe loaded using metadata options
    */
  protected def loadDataSet(): Try[DataFrame] = {

    try {
      val dfIn = loadJsonData()
      val dfInWithInputFilename = dfIn.select(
        org.apache.spark.sql.functions.input_file_name(),
        org.apache.spark.sql.functions.col("value")
      )

      logger.debug(dfIn.schema.treeString)

      val df = applyIgnore(dfInWithInputFilename)

      Success(df)
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
    val rdd: RDD[Row] = dataset.rdd

    val checkedRDD: RDD[Either[List[String], (String, String)]] = JsonIngestionUtil
      .parseRDD(rdd, schemaSparkType)
      .persist(settings.comet.cacheStorageLevel)

    val acceptedRDD: RDD[String] =
      checkedRDD.filter(_.isRight).map(_.right.get).map { case (row, inputFileName) =>
        val (left, _) = row.splitAt(row.lastIndexOf("}"))

        // Because Spark cannot detect the input files when session.read.json(session.createDataset(acceptedRDD)(Encoders.STRING)),
        // We should add it as a normal field in the RDD before converting to a dataframe using session.read.json

        s"""$left, "${Settings.cometInputFileNameColumn}" : "$inputFileName" }"""
      }

    val rejectedRDD: RDD[String] =
      checkedRDD.filter(_.isLeft).map(_.left.get.mkString("\n"))

    val appliedSchema = schema
      .sparkSchemaWithoutScriptedFields(schemaHandler)
      .add(StructField(Settings.cometInputFileNameColumn, StringType))

    val acceptedDF = session.read
      .schema(appliedSchema)
      .json(session.createDataset(acceptedRDD)(Encoders.STRING))

    val typesMap = types.map(tpe => tpe.name -> tpe).toMap
    val transformedAcceptedDF = TempUtil.run(acceptedDF, schema, typesMap)
    transformedAcceptedDF.show(false)
    saveRejected(rejectedRDD)
    saveAccepted(acceptedDF) // prefer to let Spark compute the final schema
    (rejectedRDD, acceptedDF.rdd)
  }

  override def name: String = "JsonJob"
}

object TempUtil {

  def run(acceptedDF: DataFrame, schema: Schema, types: Map[String, Type])(implicit
    settings: Settings
  ): DataFrame = {
    implicit val encoder = acceptedDF.encoder
    acceptedDF.map { row =>
      val rowWithSchema = row.asInstanceOf[GenericRowWithSchema]
      traverseRow(rowWithSchema, schema.attributesWithoutScriptedFields(), types)
    }
  }

  def traverseRow(row: GenericRowWithSchema, attributes: List[Attribute], types: Map[String, Type])(
    implicit settings: Settings
  ): Row = {
    def validateCol(attribute: Attribute, item: Any) = {
      val colResult = IngestionUtil.validateCol(
        Option(item).map(_.toString),
        attribute,
        types(attribute.`type`),
        Map.empty[String, Option[String]]
      )
      colResult.colInfo.success match {
        case true => colResult.sparkValue
        case false => null
      }
    }

    val updatedRow: Seq[Any] = attributes.map { attribute =>
      Try(row.fieldIndex(attribute.name)) map { index =>
        row.get(index) match {
          case cell: GenericRowWithSchema => // nested field
            traverseRow(cell, attribute.attributes.getOrElse(Nil), types)
          case item: mutable.WrappedArray[_] => // repeated field
            item.map {
              case subitem: GenericRowWithSchema =>
                traverseRow(subitem, attribute.attributes.getOrElse(Nil), types)
              case subitem =>
                validateCol(attribute, subitem)
            }
          case item =>
            validateCol(attribute, item)
        }
      } getOrElse(null)
    }
    new GenericRowWithSchema(updatedRow.toArray, row.schema)
  }

}
