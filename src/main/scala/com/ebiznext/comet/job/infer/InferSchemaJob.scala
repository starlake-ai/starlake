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
import com.ebiznext.comet.schema.model.Attribute
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class InferSchema(dataPath: String, savePath: String, header: Option[Boolean] = Some(false)) {

  InferSchemaJob.infer(dataPath, savePath, header.getOrElse(false))

}

object InferSchemaJob extends SparkJob {

  /** Read file without specifying the format
    *
    * @param path : file path
    * @return a dataset of string that contains data file
    */
  def readFile(path: Path) = {
    session.read
      .textFile(path.toString)
  }

  /** Get domain name
    *
    * @return the domain name
    */
  def getMode: String = "FILE"

  /** Get extension file
    *
    * @param path : file path
    * @return the file extension
    */
  def getExtensionFile(path: Path) = {
    val fileName = path.getName

    if (fileName.contains("."))
      fileName.split("\\.").last
    else
      ""
  }

  /** Get format file
    *
    * @param datasetInit : created dataset without specifying format
    * @param extension : extension file
    * @return
    */
  def getFormatFile(datasetInit: Dataset[String], extension: String): String = {
    val firstLine = datasetInit.first()

    firstLine.charAt(0).toString match {
      case "{" => "JSON"
      case "[" => "ARRAY_JSON"
      case _ =>
        if (extension.matches(".*sv$")) "DSV"
        else throw new Exception("The format of this file is not supported")
    }
  }

  /** Get separator file
    *
    * @param datasetInit : created dataset without specifying format
    * @return the file separator
    */
  def getSeparator(datasetInit: Dataset[String]) = {
    session.sparkContext
      .parallelize(datasetInit.take(10))
      .map(x => x.replaceAll("[A-Za-z0-9 \"'()?!éèîàÀÉÈç+]", ""))
      .flatMap(_.toCharArray)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .first()
      ._1
      .toString
  }

  /** Get domain name
    *
    * @param path : file path
    * @return the domain name
    */
  def getDomainName(path: Path): String = {
    path.getParent.getName
  }

  /** Get domain directory name
    *
    * @param path : file path
    * @return the domain directory name
    */
  def getDomainDirectoryName(path: Path): String = {
    path.toString.replace(path.getName, "")
  }

  /** Get schema name
    *
    * @param path : file path
    * @return the schema name
    */
  def getSchemaName(path: Path): String = {
    val fileName = path.getName

    if (fileName.contains("."))
      fileName.split("\\.").head
    else
      fileName
  }

  /** Get schema pattern
    *
    * @param path : file path
    * @return the schema pattern
    */
  def getSchemaPattern(path: Path): String = {
    path.getName
  }

  /** Get quote option
    *
    * @return the quote option
    */
  def getQuote: String = "\""

  /** Get escape option
    *
    * @return the escape option
    */
  def getEscape: String = "\\"

  def getWriteMode: String = {
    "APPEND"
  }

  /**
    *
    * @param datasetInit : created dataset without specifying format
    * @param path : file path
    * @return
    */
  def createDataFrameWithFormat(
    datasetInit: Dataset[String],
    path: Path,
    header: Boolean
  ): DataFrame = {
    val formatFile = getFormatFile(datasetInit, getExtensionFile(path))

    formatFile match {
      case "JSON" | "ARRAY_JSON" =>
        session.read
          .format("json")
          .option("inferSchema", false)
          .load(path.toString)

      case "DSV" =>
        session.read
          .format("com.databricks.spark.csv")
          .option("header", header)
          .option("inferSchema", false)
          .option("delimiter", getSeparator(datasetInit))
          .option("quote", getQuote)
          .option("escape", getEscape)
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
  def infer(dataPath: String, savePath: String, header: Boolean) = {
    val path = new Path(dataPath)

    val datasetWithoutFormat = readFile(path)

    val dataframeWithFormat = createDataFrameWithFormat(datasetWithoutFormat, path, header)

    val mode = Option(getMode)

    val format = Option(getFormatFile(datasetWithoutFormat, getExtensionFile(path)))

    val array = if (format == "ARRAY_JSON") true else false

    val withHeader = header

    val separator = getSeparator(datasetWithoutFormat)

    val quote = getQuote

    val escape = getEscape

    val writeMode = getWriteMode

    val inferSchema = new InferSchemaHandler(dataframeWithFormat)

    val attributes: List[Attribute] = inferSchema.createAttributes(dataframeWithFormat.schema)

    val metadata = inferSchema.createMetaData(
      mode,
      format,
      None, //multiline is not supported
      Option(array),
      Option(withHeader),
      Option(separator),
      Option(quote),
      Option(escape),
      Option(writeMode),
      None,
      None
    )

    val schema = inferSchema.createSchema(
      getSchemaName(path),
      Pattern.compile(getSchemaPattern(path)),
      attributes,
      Some(metadata)
    )

    val domain =
      inferSchema.createDomain(
        getDomainName(path),
        getDomainDirectoryName(path),
        None,
        List(schema) // todo add support to iterate over all the files in a directory
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
