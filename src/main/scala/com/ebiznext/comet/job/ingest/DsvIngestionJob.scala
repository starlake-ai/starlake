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

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult, RowInfo, RowResult}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

/**
  * Main class to ingest delimiter separated values file
  *
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
  */
class DsvIngestionJob(
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends IngestionJob {

  /**
    *
    * @return Spark Job name
    */
  override def name: String =
    s"""${domain.name}-${schema.name}-${path.map(_.getName).mkString(",")}"""

  /**
    * dataset Header names as defined by the schema
    */
  val schemaHeaders: List[String] = schema.attributes.map(_.name)

  /**
    * remove any extra quote / BOM in the header
    *
    * @param header : Header column name
    * @return
    */
  def cleanHeaderCol(header: String): String =
    header.replaceAll("\"", "").replaceAll("\uFEFF", "")

  /**
    *
    * @param datasetHeaders : Headers found in the dataset
    * @param schemaHeaders  : Headers defined in the schema
    * @return success  if all headers in the schema exist in the dataset
    */
  def validateHeader(datasetHeaders: List[String], schemaHeaders: List[String]): Boolean = {
    schemaHeaders.forall(schemaHeader => datasetHeaders.contains(schemaHeader))
  }

  /**
    *
    * @param datasetHeaders : Headers found in the dataset
    * @param schemaHeaders  : Headers defined in the schema
    * @return two lists : One with thecolumns present in the schema and the dataset and onther with the headers present in the dataset only
    */
  def intersectHeaders(
    datasetHeaders: List[String],
    schemaHeaders: List[String]
  ): (List[String], List[String]) = {
    datasetHeaders.partition(schemaHeaders.contains)
  }

  /**
    * Load dataset using spark csv reader and all metadata. Does not infer schema.
    * columns not defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return Spark Dataset
    */
  def loadDataSet(): Try[DataFrame] = {
    try {
      val df = session.read
        .option("header", metadata.isWithHeader().toString)
        .option("inferSchema", value = false)
        .option("delimiter", metadata.getSeparator())
        .option("multiLine", metadata.getMultiline())
        .option("quote", metadata.getQuote())
        .option("escape", metadata.getEscape())
        .option("parserLib", "UNIVOCITY")
        .option("encoding", metadata.getEncoding())
        .csv(path.map(_.toString): _*)
      df.printSchema()

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
          df.toDF(
            schema.attributes
              .map(_.name)
              .take(Math.min(df.columns.length, schema.attributes.length)): _*
          )
      }
      Success(resDF)
    } catch {
      case e: Exception =>
        Failure(e)
    }

  }

  /**
    * Apply the schema to the dataset. This is where all the magic happen
    * Valid records are stored in the accepted path / table and invalid records in the rejected path / table
    *
    * @param dataset : Spark Dataset
    */
  def ingest(dataset: DataFrame): (RDD[_], RDD[_]) = {
    def reorderAttributes(): List[Attribute] = {
      val attributesMap =
        this.schema.attributes.map(attr => (attr.name, attr)).toMap
      dataset.columns.map(colName => attributesMap(colName)).toList
    }

    val orderedAttributes = reorderAttributes()

    def reorderTypes(): (List[Type], StructType) = {
      val typeMap: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
      val (tpes, sparkFields) = orderedAttributes.map { attribute =>
        val tpe = typeMap(attribute.`type`)
        (tpe, tpe.sparkType(attribute.name, !attribute.required, attribute.comment))
      }.unzip
      (tpes, StructType(sparkFields))
    }

    val (orderedTypes, orderedSparkTypes) = reorderTypes()

    val (rejectedRDD, acceptedRDD) = DsvIngestionUtil.validate(
      session,
      dataset,
      orderedAttributes,
      orderedTypes,
      orderedSparkTypes
    )
    saveRejected(rejectedRDD)

    val (df, _) = saveAccepted(acceptedRDD, orderedSparkTypes)
    index(df)
    (rejectedRDD, acceptedRDD)
  }

  def saveAccepted(acceptedRDD: RDD[Row], orderedSparkTypes: StructType): (DataFrame, Path) = {
    val renamedAttributes = schema.renamedAttributes().toMap
    logger.whenInfoEnabled {
      renamedAttributes.foreach {
        case (name, rename) =>
          logger.info(s"renaming column $name to $rename")
      }
    }
    val acceptedDF = session.createDataFrame(acceptedRDD, orderedSparkTypes)
    val cols = acceptedDF.columns.map { column =>
      org.apache.spark.sql.functions
        .col(column)
        .as(renamedAttributes.getOrElse(column, column))
    }
    val finalDF = acceptedDF.select(cols: _*)
    super.saveAccepted(finalDF)
  }

}

/**
  * The Spark task that run on each worker
  */
object DsvIngestionUtil {

  /**
    * For each col of each row
    *   - we extract the col value / the col constraints / col type
    *   - we check that the constraints are verified
    *   - we apply any required privacy transformation
    *   - parse the column into the target primitive Spark Type
    * We end up using catalyst to create a Spark Row
    *
    * @param session    : The Spark session
    * @param dataset    : The dataset
    * @param attributes : the col attributes
    * @param types      : List of globally defined types
    * @param sparkType  : The expected Spark Type for valid rows
    * @return Two RDDs : One RDD for rejected rows and one RDD for accepted rows
    */
  def validate(
    session: SparkSession,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType
  )(implicit settings: Settings): (RDD[String], RDD[Row]) = {
    val now = Timestamp.from(Instant.now)
    val checkedRDD: RDD[RowResult] = dataset.rdd.mapPartitions { partition =>
        partition.map { row: Row =>
          val rowCols = row.toSeq
            .zip(attributes)
            .map {
              case (colValue, colAttribute) =>
                (Option(colValue).getOrElse("").toString, colAttribute)
            }
            .zip(types)
          val validNumberOfColumns = attributes.length <= rowCols.length
          if (!validNumberOfColumns) {
            RowResult(
              rowCols.map {
                case ((colRawValue, colAttribute), tpe) =>
                  ColResult(
                    ColInfo(
                      colRawValue,
                      colAttribute.name,
                      tpe.name,
                      tpe.pattern,
                      false
                    ),
                    null
                  )
              }.toList
            )
          } else {
            RowResult(
              rowCols.map {
                case ((colRawValue, colAttribute), tpe) =>
                  IngestionUtil.validateCol(colRawValue, colAttribute, tpe)
              }.toList
            )
          }
        }
      } cache ()

    val rejectedRDD: RDD[String] = checkedRDD
      .filter(_.isRejected)
      .map(rr => RowInfo(now, rr.colResults.filter(!_.colInfo.success).map(_.colInfo)).toString)

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues: _*).toSeq.toArray, sparkType)
    }
    (rejectedRDD, acceptedRDD)
  }
}
