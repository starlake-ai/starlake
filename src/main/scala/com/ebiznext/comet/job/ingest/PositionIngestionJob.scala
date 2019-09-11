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

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.Rejection.{RowInfo, RowResult}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
class PositionIngestionJob(
  domain: Domain,
  schema: Schema,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler
) extends DsvIngestionJob(domain, schema, types, path, storageHandler) {

  /**
    * Load dataset using spark csv reader and all metadata. Does not infer schema.
    * columns not defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return Spark DataFrame where each row holds a single string
    */
  override def loadDataSet(): Try[DataFrame] = {
    try {
      val df =
        session.read.option("encoding", metadata.getEncoding()).text(path.map(_.toString): _*)
      metadata.withHeader match {
        case Some(true) =>
          Failure(new Exception("No Header allowed for Position File Format "))
        case Some(false) | None =>
          Success(df)
      }
    } catch {
      case e: Exception =>
        Failure(e)
    }

  }

  /**
    * Apply the schema to the dataset. This is where all the magic happen
    * Valid records are stored in the accepted path / table and invalid records in the rejected path / table
    *
    * @param input : Spark Dataset
    */
  override def ingest(input: DataFrame): (RDD[_], RDD[_]) = {

    val dataset: DataFrame = PositionIngestionUtil.prepare(session, input, schema.attributes)

    def reorderAttributes(): List[Attribute] = {
      val attributesMap =
        this.schema.attributes.map(attr => (attr.name, attr)).toMap
      dataset.columns.map(colName => attributesMap(colName)).toList
    }

    val orderedAttributes = reorderAttributes()

    def reorderTypes(): (List[Type], StructType) = {
      val mapTypes: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
      val (tpes, sparkFields) = orderedAttributes.map { attribute =>
        val tpe = mapTypes(attribute.`type`)
        (tpe, tpe.sparkType(attribute.name, !attribute.required, attribute.comment))
      }.unzip
      (tpes, StructType(sparkFields))
    }

    val (orderedTypes, orderedSparkTypes) = reorderTypes()

    val (rejectedRDD, acceptedRDD) = PositionIngestionUtil.validate(
      session,
      dataset,
      orderedAttributes,
      orderedTypes,
      orderedSparkTypes
    )
    saveRejected(rejectedRDD)
    saveAccepted(acceptedRDD, orderedSparkTypes)
    (rejectedRDD, acceptedRDD)
  }

}

/**
  * The Spark task that run on each worker
  */
object PositionIngestionUtil {

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

    val positions = attributes.map(_.position.get)
    val fieldTypeArray = new Array[StructField](positions.length)
    for (i <- attributes.indices) {
      fieldTypeArray(i) = StructField(s"col$i", StringType)
    }
    val rdd = input.rdd.map { row =>
      getRow(row.getString(0), positions)
    }

    val dataset =
      session.createDataFrame(rdd, StructType(fieldTypeArray)).toDF(attributes.map(_.name): _*)
    dataset
  }

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
  ): (RDD[String], RDD[Row]) = {
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
        RowResult(
          rowCols.map {
            case ((colRawValue, colAttribute), tpe) =>
              IngestionUtil.validateCol(validNumberOfColumns, colRawValue, colAttribute, tpe)
          }.toList
        )
      }
    } cache ()

    val rejectedRDD: RDD[String] = checkedRDD
      .filter(_.isRejected)
      .map(rr => RowInfo(now, rr.colResults.map(_.colInfo)).toString)

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues: _*).toSeq.toArray, sparkType)
    }
    (rejectedRDD, acceptedRDD)
  }
}
