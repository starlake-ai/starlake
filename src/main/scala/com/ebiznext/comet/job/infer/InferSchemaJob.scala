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

package com.ebiznext.comet.job.infer

import java.util.regex.Pattern

import com.ebiznext.comet.schema.handlers.InferSchemaHandler
import com.ebiznext.comet.schema.model.{Attribute, Domain}
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** *
  *
  * @param domainName : name of the domain
  * @param schemaName : name of the schema
  * @param dataPath   : path to the dataset to infer schema from. format is /path/to/read
  * @param savePath   : path to save the yaml file. format is /path/to/save
  * @param header     : option of boolean to check if header should be included (false by default)
  */
class InferSchema(
                   domainName: String,
                   schemaName: String,
                   dataPath: String,
                   savePath: String,
                   header: Option[Boolean] = Some(false)
                 ){

  InferSchemaJob.infer(domainName, schemaName, dataPath, savePath, header.getOrElse(false))

}

/** *
  * Infers the schema of a given datapath, domain name, schema name.
  */
object InferSchemaJob extends SparkJob{

  /**
    * Read file without specifying the format
    *
    * @param path : file path
    * @return a dataset of string that contains data file
    */
  def readFile(path: Path): Dataset[String] = {
    session.read
      .textFile(path.toString)
  }

  /**
    * Get format file by using the first and the last line of the dataset
    * We use mapPartitionsWithIndex to retrieve these informations to make sure that the first line really corresponds to the first line (same for the last)
    *
    * @param datasetInit : created dataset without specifying format
    * @return
    */
  def getFormatFile(datasetInit: Dataset[String]): String = {
    val rddDatasetInit = datasetInit.rdd
    val lastPartitionNo = rddDatasetInit.getNumPartitions - 1

    //Retrieve the first and the last line of a dataset
    val partitionWithIndex = rddDatasetInit.mapPartitionsWithIndex { (index, iterator) => {
      val iteratorList = iterator.toList
      //The first line is stored into the 0th partition
      //Check if data is stored on the same partition (if true return directly the first and the last line)
      if (index == 0 & lastPartitionNo == 0) {
        Iterator(iteratorList.take(1).head, iteratorList.reverse.take(1).last)
      } else if (index == 0 & lastPartitionNo != 0) {
        iteratorList.take(1).iterator
      }
      //The last line is stored into the last partition
      else if (index == lastPartitionNo) {
        iteratorList.reverse.take(1).iterator
      } else
        Iterator()
    }
    }

    val firstLine = partitionWithIndex.first
    val lastLine = partitionWithIndex.collect().last

    if (firstLine.startsWith("{") & firstLine.endsWith("}")) "JSON"
    else if (firstLine.startsWith("[") & lastLine.endsWith("]")) "ARRAY_JSON"
    else "DSV"

  }

  /**
    * Get separator file by taking the character that appears the most in 10 lines of the dataset
    *
    * @param datasetInit : created dataset without specifying format
    * @return the file separator
    */
  def getSeparator(datasetInit: Dataset[String]): String = {
    session.sparkContext
      .parallelize(datasetInit.take(10))
      .map(x => x.replaceAll("[A-Za-z0-9 \"'()@?!éèîàÀÉÈç+]", ""))
      .flatMap(_.toCharArray)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .max
      ._1
      .toString
  }

  /**
    * Get domain directory name
    *
    * @param path : file path
    * @return the domain directory name
    */
  def getDomainDirectoryName(path: Path): String = {
    path.toString.replace(path.getName, "")
  }

  /**
    * Get schema pattern
    *
    * @param path : file path
    * @return the schema pattern
    */
  def getSchemaPattern(path: Path): String = {
    path.getName
  }

  /**
    * Create the dataframe with its associated format
    *
    * @param datasetInit : created dataset without specifying format
    * @param path        : file path
    * @return
    */
  def createDataFrameWithFormat(
                                 datasetInit: Dataset[String],
                                 path: Path,
                                 header: Boolean
                               ): DataFrame = {
    val formatFile = getFormatFile(datasetInit)

    formatFile match {
      case "JSON" | "ARRAY_JSON" =>
        session.read
          .format("json")
          .option("inferSchema", value = true)
          .load(path.toString)

      case "DSV" =>
        session.read
          .format("com.databricks.spark.csv")
          .option("header", header)
          .option("inferSchema", value = true)
          .option("delimiter", getSeparator(datasetInit))
          .option("parserLib", "UNIVOCITY")
          .load(path.toString)
    }
  }

  override def name: String = "InferSchema"

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  def infer(
             domainName: String,
             schemaName: String,
             dataPath: String,
             savePath: String,
             header: Boolean
           ): Unit = {
    val path = new Path(dataPath)

    val datasetWithoutFormat = readFile(path)

    val dataframeWithFormat = createDataFrameWithFormat(datasetWithoutFormat, path, header)

    val format = Option(getFormatFile(datasetWithoutFormat))

    val array = if (format.getOrElse("") == "ARRAY_JSON") true else false

    val withHeader = header

    val separator = getSeparator(datasetWithoutFormat)

    val inferSchema = InferSchemaHandler

    val attributes: List[Attribute] = inferSchema.createAttributes(dataframeWithFormat.schema)

    val metadata = inferSchema.createMetaData(
      format,
      Option(array),
      Option(withHeader),
      Option(separator)
    )

    val schema = inferSchema.createSchema(
      schemaName,
      Pattern.compile(getSchemaPattern(path)),
      attributes,
      Some(metadata)
    )

    val domain: Domain =
      inferSchema.createDomain(
        domainName,
        getDomainDirectoryName(path),
        schemas = List(schema)
      )

    inferSchema.generateYaml(domain, savePath)
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): SparkSession = session
}
