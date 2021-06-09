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

import com.ebiznext.comet.config.{Settings, SparkEnv}
import com.ebiznext.comet.schema.handlers.InferSchemaHandler
import com.ebiznext.comet.schema.model.{Attribute, Domain}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import java.util.regex.Pattern
import scala.io.Source
import scala.util.Try

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
)(implicit settings: Settings) {

  def run(): Try[Unit] =
    (new InferSchemaJob).infer(domainName, schemaName, dataPath, savePath, header.getOrElse(false))

}

/** *
  * Infers the schema of a given datapath, domain name, schema name.
  */
class InferSchemaJob(implicit settings: Settings) {

  def name: String = "InferSchema"

  private val sparkEnv: SparkEnv = new SparkEnv(name)
  private val session: SparkSession = sparkEnv.session

  /** Read file without specifying the format
    *
    * @param path : file path
    * @return a dataset of string that contains data file
    */
  def readFile(path: Path): Dataset[String] = {
    session.read
      .textFile(path.toString)
  }

  /** Get format file by using the first and the last line of the dataset
    * We use mapPartitionsWithIndex to retrieve these information to make sure that the first line really corresponds to the first line (same for the last)
    *
    * @param lines : list of lines read from file
    * @return
    */
  def getFormatFile(lines: List[String]): String = {
    val firstLine = lines.head
    val lastLine = lines.last

    val jsonRegexStart = """\{.*""".r
    val jsonArrayRegexStart = """\[.*""".r

    val jsonRegexEnd = """.*\}""".r
    val jsonArrayRegexEnd = """.*\]""".r

    val xmlRegexStart = """<.*""".r
    val xmlRegexEnd = """.*>""".r

    (firstLine, lastLine) match {
      case (jsonRegexStart(), jsonRegexEnd())           => "JSON"
      case (jsonArrayRegexStart(), jsonArrayRegexEnd()) => "ARRAY_JSON"
      case (xmlRegexStart(), xmlRegexEnd())             => "XML"
      case _                                            => "DSV"
    }
  }

  /** Get separator file by taking the character that appears the most in 10 lines of the dataset
    *
    * @param lines : list of lines read from file
    * @return the file separator
    */
  def getSeparator(lines: List[String]): String = {
    val firstLine = lines.head
    val separator =
      firstLine
        .replaceAll("[A-Za-z0-9 \"'()@?!éèîàÀÉÈç+]", "")
        .toCharArray
        .map((_, 1))
        .groupBy(_._1)
        .mapValues(_.size)
        .toList
        .maxBy(_._2)
    separator._1.toString
  }

  /** Get domain directory name
    *
    * @param path : file path
    * @return the domain directory name
    */
  def getDomainDirectoryName(path: Path): String = {
    path.toString.replace(path.getName, "")
  }

  /** Get schema pattern
    *
    * @param path : file path
    * @return the schema pattern
    */
  def getSchemaPattern(path: Path): String = {
    path.getName
  }

  /** Create the dataframe with its associated format
    *
    * @param lines : list of lines read from file
    * @param path        : file path
    * @return
    */
  def createDataFrameWithFormat(
    lines: List[String],
    dataPath: String,
    header: Boolean
  ): DataFrame = {
    val formatFile = getFormatFile(lines)
    formatFile match {
      case "ARRAY_JSON" =>
        val jsonRDD =
          session.sparkContext.wholeTextFiles(dataPath).map { case (_, content) =>
            content
          }
        session.read
          .option("inferSchema", value = true)
          .json(session.createDataset(jsonRDD)(Encoders.STRING))

      case "JSON" =>
        session.read
          .format("json")
          .option("inferSchema", value = true)
          .load(dataPath)

      case "XML" =>
        session.read
          .format("xml")
          .option("inferSchema", value = true)
          .load(dataPath)

      case "DSV" =>
        session.read
          .format("com.databricks.spark.csv")
          .option("header", header)
          .option("inferSchema", value = true)
          .option("delimiter", getSeparator(lines))
          .option("parserLib", "UNIVOCITY")
          .load(dataPath)
    }
  }

  /** Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  def infer(
    domainName: String,
    schemaName: String,
    dataPath: String,
    savePath: String,
    header: Boolean
  ): Try[Unit] = {
    Try {
      val path = new Path(dataPath)

      val lines = Source.fromFile(path.toString).getLines().toList.map(_.trim).filter(_.nonEmpty)

      val dataframeWithFormat = createDataFrameWithFormat(lines, dataPath, header)

      val format = getFormatFile(lines)

      val array = format == "ARRAY_JSON"

      val withHeader = header

      val separator = getSeparator(lines)

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
  }
}
