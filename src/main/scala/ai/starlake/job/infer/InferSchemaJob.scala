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

package ai.starlake.job.infer

import ai.starlake.config.{DatasetArea, Settings, SparkEnv}
import ai.starlake.schema.handlers.InferSchemaHandler
import ai.starlake.schema.model.{Attribute, Domain, Format, Position}
import better.files.File
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import java.util.regex.Pattern
import scala.io.Source
import scala.util.Try

/** *
  *
  * @param domainName
  *   : name of the domain
  * @param schemaName
  *   : name of the schema
  * @param dataPath
  *   : path to the dataset to infer schema from. format is /path/to/read
  * @param savePath
  *   : path to save the yaml file. format is /path/to/save
  * @param header
  *   : option of boolean to check if header should be included (false by default)
  */
class InferSchema(
  domainName: String,
  schemaName: String,
  pattern: Option[String],
  comment: Option[String],
  dataPath: String,
  saveDir: String,
  header: Boolean = false,
  format: Option[Format] = None
)(implicit settings: Settings) {
  def run(): Try[File] = {
    val dir = if (saveDir.isEmpty) DatasetArea.load.toString else saveDir
    (new InferSchemaJob).infer(
      domainName = domainName,
      schemaName = schemaName,
      pattern = pattern,
      comment = comment,
      dataPath = dataPath,
      saveDir = dir,
      withHeader = header,
      forceFormat = format
    )
  }

}

/** * Infers the schema of a given datapath, domain name, schema name.
  */
class InferSchemaJob(implicit settings: Settings) {

  def name: String = "InferSchema"

  private val sparkEnv: SparkEnv = new SparkEnv(name)
  private val session: SparkSession = sparkEnv.session

  /** Read file without specifying the format
    *
    * @param path
    *   : file path
    * @return
    *   a dataset of string that contains data file
    */
  def readFile(path: Path): Dataset[String] = {
    session.read
      .textFile(path.toString)
  }

  /** Get format file by using the first and the last line of the dataset We use
    * mapPartitionsWithIndex to retrieve these information to make sure that the first line really
    * corresponds to the first line (same for the last)
    *
    * @param lines
    *   : list of lines read from file
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
    * @param lines
    *   : list of lines read from file
    * @return
    *   the file separator
    */
  def getSeparator(lines: List[String], withHeader: Boolean): String = {
    val linesWithoutHeader = if (withHeader) lines.drop(1) else lines
    val firstLine = linesWithoutHeader.head
    val (separator, count) =
      firstLine
        .replaceAll("[A-Za-z0-9 \"'()@?!éèîàÀÉÈç+\\-_]", "")
        .toCharArray
        .map((_, 1))
        .groupBy(_._1)
        .mapValues(_.length)
        .toList
        .maxBy { case (ch, count) => count }
    separator.toString
  }

  /** Get domain directory name
    *
    * @param path
    *   : file path
    * @return
    *   the domain directory name
    */
  def getDomainDirectoryName(path: Path): String = {
    path.toString.replace(path.getName, "")
  }

  /** Get schema pattern
    *
    * @param path
    *   : file path
    * @return
    *   the schema pattern
    */
  private def getSchemaPattern(path: Path): String = {
    val filename = path.getName
    val parts = filename.split("\\.")
    if (parts.length < 2)
      filename
    else {
      val extension = parts.last
      val prefix = filename.replace(s".$extension", "")
      val indexOfNonAlpha = prefix.indexWhere(!_.isLetterOrDigit)
      val prefixWithoutNonAlpha = prefix.substring(0, indexOfNonAlpha)
      if (prefixWithoutNonAlpha.isEmpty)
        filename
      else
        s"$prefixWithoutNonAlpha.*.$extension"
    }
  }

  /** Create the dataframe with its associated format
    *
    * @param lines
    *   : list of lines read from file
    * @param path
    *   : file path
    * @return
    */
  private def createDataFrameWithFormat(
    lines: List[String],
    dataPath: String,
    withHeader: Boolean
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
          .format("com.databricks.spark.xml")
          .option("inferSchema", value = true)
          .load(dataPath)

      case "DSV" =>
        session.read
          .format("com.databricks.spark.csv")
          .option("header", withHeader)
          .option("inferSchema", value = true)
          .option("delimiter", getSeparator(lines, withHeader))
          .option("parserLib", "UNIVOCITY")
          .load(dataPath)
    }
  }

  /** Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  def infer(
    domainName: String,
    schemaName: String,
    pattern: Option[String],
    comment: Option[String],
    dataPath: String,
    saveDir: String,
    withHeader: Boolean,
    forceFormat: Option[Format]
  ): Try[File] = {
    Try {
      val path = new Path(dataPath)

      val lines = Source.fromFile(path.toString).getLines().toList.map(_.trim).filter(_.nonEmpty)

      val schema = forceFormat match {
        case Some(Format.POSITION) =>
          var lastIndex = -1
          val attributes = lines.zipWithIndex.map { case (line, index) =>
            val startPosition = lastIndex + 1
            val endPosition = startPosition + line.length
            lastIndex = endPosition
            Attribute(
              name = s"_c$index",
              position = Some(Position(startPosition, endPosition))
            )
          }
          val metadata = InferSchemaHandler.createMetaData(Format.POSITION)
          InferSchemaHandler.createSchema(
            schemaName,
            Pattern.compile(pattern.getOrElse(getSchemaPattern(path))),
            comment,
            attributes,
            Some(metadata)
          )
        case forceFormat =>
          val dataframeWithFormat = createDataFrameWithFormat(lines, dataPath, withHeader)

          val (format, array) = forceFormat match {
            case None =>
              val formatAsStr = getFormatFile(lines)
              (Format.fromString(getFormatFile(lines)), formatAsStr == "ARRAY_JSON")
            case Some(f) => (f, false)
          }

          val (dataLines, separator) =
            format match {
              case Format.DSV =>
                val separator = getSeparator(lines, withHeader)
                val linesWithoutHeader = if (withHeader) lines.drop(1) else lines
                (linesWithoutHeader.map(_.split(Pattern.quote(separator))), Some(separator))
              case _ => (Nil, None)
            }

          val attributes: List[Attribute] =
            InferSchemaHandler.createAttributes(dataLines, dataframeWithFormat.schema, format)

          val metadata = InferSchemaHandler.createMetaData(
            format,
            Option(array),
            Option(withHeader),
            separator
          )

          InferSchemaHandler.createSchema(
            schemaName,
            Pattern.compile(pattern.getOrElse(getSchemaPattern(path))),
            comment,
            attributes,
            Some(metadata)
          )
      }

      val domain: Domain =
        InferSchemaHandler.createDomain(
          domainName,
          schema.metadata.map(_.copy(directory = Some(s"{{root_path}}/incoming/$domainName"))),
          schemas = List(schema)
        )

      InferSchemaHandler.generateYaml(domain, saveDir)
    }.flatten
  }
}
