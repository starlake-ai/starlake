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

import ai.starlake.config.{Settings, SparkEnv}
import ai.starlake.schema.handlers.{InferSchemaHandler, StorageHandler}
import ai.starlake.schema.model.Format.DSV
import ai.starlake.schema.model._
import better.files.File
import com.google.cloud.spark.bigquery.repackaged.org.json.JSONArray
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.BufferedReader
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.util.Try

/** * Infers the schema of a given datapath, domain name, schema name.
  */
class InferSchemaJob(implicit settings: Settings) extends StrictLogging {

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
  def getFormatFile(inputPath: String, lines: List[String]): String = {
    val file = File(inputPath)
    val firstLine = lines.head
    val lastLine = lines.last

    file.extension(includeDot = false).getOrElse("").toLowerCase() match {
      case "xml" => "XML"
      case "json" if firstLine.startsWith("[") =>
        "JSON_ARRAY"
      case "json" if firstLine.startsWith("{") =>
        "JSON"
      case "csv" | "dsv" | "tsv" | "psv" => "DSV"
      case _ =>
        val jsonRegexStart = """\{.*""".r
        val jsonArrayRegexStart = """\[.*""".r

        val jsonRegexEnd = """.*\}""".r
        val jsonArrayRegexEnd = """.*\]""".r

        val xmlRegexStart = """<.*""".r
        val xmlRegexEnd = """.*>""".r

        (firstLine, lastLine) match {
          case (jsonRegexStart(), jsonRegexEnd())           => "JSON"
          case (jsonArrayRegexStart(), jsonArrayRegexEnd()) => "JSON_ARRAY"
          case (xmlRegexStart(), xmlRegexEnd())             => "XML"
          case _                                            => "DSV"
        }
    }
  }

  /** Get separator file by taking the character that appears the most in 10 lines of the dataset
    *
    * @param lines
    *   : list of lines read from file
    * @return
    *   the file separator
    */
  def getSeparator(lines: List[String]): String = {
    val linesWithoutHeader = lines.drop(1)
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
      val indexOfNonAlpha = prefix.lastIndexWhere(!_.isLetterOrDigit)
      val prefixWithoutNonAlpha =
        if (
          indexOfNonAlpha != -1 &&
          indexOfNonAlpha < prefix.length &&
          prefix(indexOfNonAlpha + 1).isDigit
        )
          prefix.substring(0, indexOfNonAlpha)
        else prefix
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
    content: String,
    tableName: String,
    rowTag: Option[String],
    inferSchema: Boolean = true
  ): (DataFrame, Option[String]) = {
    val formatFile = getFormatFile(dataPath, lines)

    formatFile match {
      case "JSON_ARRAY" =>
        val content = lines.mkString("\n")
        val jsons = ListBuffer[String]()
        val jsonarray = new JSONArray(content)
        for (i <- 0 until jsonarray.length) {
          val jsonobject = jsonarray.getJSONObject(i)
          jsons.append(jsonobject.toString)
        }

        val tmpFile = File.newTemporaryFile()
        tmpFile.write(jsons.mkString("\n"))
        tmpFile.deleteOnExit()
        val df = session.read
          .format("json")
          .option("inferSchema", value = inferSchema)
          .load(tmpFile.pathAsString)
        (df, None)
      case "JSON" =>
        val df = session.read
          .format("json")
          .option("inferSchema", value = inferSchema)
          .load(dataPath)
        (df, None)
      case "XML" =>
        // find second occurrence of xml tag starting with letter in content
        val tag = {
          rowTag.getOrElse {
            val contentWithoutXmlHeaderTag = content.replace("<?", "")
            val secondXmlTagStart = contentWithoutXmlHeaderTag
              .indexOf("<", contentWithoutXmlHeaderTag.indexOf("<") + 1) + 1
            val closingTag = contentWithoutXmlHeaderTag.indexOf(">", secondXmlTagStart)
            val result =
              if (secondXmlTagStart == -1 || closingTag == -1)
                tableName
              else {
                val rowTag = contentWithoutXmlHeaderTag.substring(secondXmlTagStart, closingTag)
                rowTag
              }
            logger.info(s"Using rowTag: $result")
            result
          }
        }

        val df = session.read
          .format("com.databricks.spark.xml")
          .option("rowTag", tag)
          .option("inferSchema", value = inferSchema)
          .load(dataPath)
        (df, Some(tag))
      case "DSV" =>
        val df = session.read
          .format("com.databricks.spark.csv")
          .option("header", value = true)
          .option("inferSchema", value = inferSchema)
          .option("delimiter", getSeparator(lines))
          .option("parserLib", "UNIVOCITY")
          .load(dataPath)
        (df, None)
    }
  }

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  def infer(
    domainName: String,
    tableName: String,
    pattern: Option[String],
    comment: Option[String],
    inputPath: String,
    saveDir: String,
    forceFormat: Option[Format],
    writeMode: WriteMode,
    rowTag: Option[String],
    clean: Boolean
  )(implicit storageHandler: StorageHandler): Try[Path] = {
    Try {
      val path = new Path(inputPath)
      val content = storageHandler.readAndExecute(path)(isr => {
        val bufferedReader = new BufferedReader(isr)
        (Iterator continually bufferedReader.readLine takeWhile (_ != null)).toList
      })
      val lines = content.map(_.trim).filter(_.nonEmpty)

      val schema = forceFormat match {
        case Some(Format.POSITION) =>
          var lastIndex = -1
          val attributes = lines.zipWithIndex.map { case (line, index) =>
            val fieldIndex = line.indexOf(":")
            if (fieldIndex == -1)
              throw new IllegalArgumentException(
                s"""Positional format schema inference requires a colon (:) to separate the field name from its value in line $index.
                   |Example
                   |-------
                   |order_id:00001
                   |customer_id:010203
                   |""".stripMargin
              )
            val fieldName = line.substring(0, fieldIndex).trim
            val field =
              line.substring(fieldIndex + 1) // no trim to keep leading and trailing spaces
            val startPosition = lastIndex + 1
            val endPosition = startPosition + field.length
            lastIndex = endPosition
            Attribute(name = fieldName, position = Some(Position(startPosition, endPosition)))
          }
          val metadata = InferSchemaHandler.createMetaData(Format.POSITION)
          InferSchemaHandler.createSchema(
            tableName,
            Pattern.compile(pattern.getOrElse(getSchemaPattern(path))),
            comment,
            attributes,
            Some(metadata)
          )
        case forceFormat =>
          val (dataframeWithFormat, xmlTag) =
            createDataFrameWithFormat(lines, inputPath, content.mkString("\n"), tableName, rowTag)

          val (format, array) = forceFormat match {
            case None =>
              val formatAsStr = getFormatFile(inputPath, lines)
              (Format.fromString(formatAsStr), formatAsStr == "JSON_ARRAY")
            case Some(f) => (f, false)
          }

          val dataLines =
            format match {
              case Format.DSV =>
                val (rawDataframeWithFormat, _) = createDataFrameWithFormat(
                  lines,
                  inputPath,
                  content.mkString("\n"),
                  tableName,
                  rowTag,
                  inferSchema = false
                )
                rawDataframeWithFormat.collect().toList
              case _ =>
                dataframeWithFormat
                  .collect()
                  .toList
            }

          val attributes: List[Attribute] =
            InferSchemaHandler.createAttributes(dataLines, dataframeWithFormat.schema, format)

          val xmlOptions = xmlTag.map(tag => Map("rowTag" -> tag))
          val metadata = InferSchemaHandler.createMetaData(
            format,
            Option(array),
            Some(true),
            format match {
              case DSV => Some(getSeparator(lines))
              case _   => None
            },
            xmlOptions
          )

          val strategy = WriteStrategy(
            `type` = Some(WriteStrategyType.fromWriteMode(writeMode))
          )

          InferSchemaHandler.createSchema(
            tableName,
            Pattern.compile(pattern.getOrElse(getSchemaPattern(path))),
            comment,
            attributes,
            Some(metadata.copy(writeStrategy = Some(strategy)))
          )
      }

      val domain: Domain =
        InferSchemaHandler.createDomain(
          domainName,
          Some(
            Metadata(
              directory = Some(s"{{incoming_path}}/$domainName")
            )
          ),
          schemas = List(schema)
        )

      InferSchemaHandler.generateYaml(domain, saveDir, clean)
    }.flatten
  }
}
