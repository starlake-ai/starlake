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
import ai.starlake.schema.handlers.{DataTypesToInt, InferSchemaHandler, StorageHandler}
import ai.starlake.schema.model.Format.DSV
import ai.starlake.schema.model._
import better.files.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.io.BufferedReader
import java.util.regex.Pattern
import scala.util.Try

/** * Infers the schema of a given datapath, domain name, schema name.
  */
class InferSchemaJob(implicit settings: Settings) extends StrictLogging {

  def name: String = "InferSchema"

  private val sparkEnv: SparkEnv = SparkEnv.get(name, identity, settings)
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
  def getFormatFile(inputPath: String)(implicit storageHandler: StorageHandler): String = {
    val file = File(inputPath)

    val filePath = new Path(inputPath)

    file.extension(includeDot = false).getOrElse("").toLowerCase() match {
      case "parquet" => "PARQUET"
      case "xml"     => "XML"
      case "json" if filePath.firstLine.startsWith("[") =>
        "JSON_ARRAY"
      case "json" if filePath.firstLine.startsWith("{") =>
        "JSON"
      case "csv" | "dsv" | "tsv" | "psv" => "DSV"
      case _ =>
        val jsonRegexStart = """\{.*""".r
        val jsonArrayRegexStart = """\[.*""".r

        val jsonRegexEnd = """.*\}""".r
        val jsonArrayRegexEnd = """.*\]""".r

        val xmlRegexStart = """<.*""".r
        val xmlRegexEnd = """.*>""".r

        (filePath.firstLine, filePath.lastLine) match {
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
  def getSeparator(firstLine: String): String = {
    val (separator, _) = {
      val chars = firstLine
        .replaceAll("[A-Za-z0-9 \"'()@?!éèîàÀÉÈç+\\-_]", "")
        .toCharArray
        .map((_, 1))
        .groupBy(_._1)
        .mapValues(_.length)
        .toList

      if (chars.nonEmpty)
        chars.maxBy { case (ch, count) => count }
      else
        // We assume "," is the separator
        (',', 1)
    }
    separator.toString
  }

  /** Get schema pattern
    *
    * @param path
    *   : file path
    * @return
    *   the schema pattern
    */

  /** Create the dataframe with its associated format
    *
    * @param lines
    *   : list of lines read from file
    * @param path
    *   : file path
    * @return
    *   dataframe and rowtag if xml
    */
  private def createDataFrameWithFormat(
    dataPath: String,
    tableName: String,
    rowTag: Option[String],
    inferSchema: Boolean = true,
    forceFormat: Option[Format] = None
  )(implicit storageHandler: StorageHandler): (DataFrame, Option[String], String) = {
    val formatFile = forceFormat.map(_.toString).getOrElse(getFormatFile(dataPath))

    formatFile match {
      case "PARQUET" =>
        val df = session.read
          .parquet(dataPath)
        (df, None, formatFile)
      case "JSON_ARRAY" =>
        val df = session.read
          .option("multiLine", true)
          .json(dataPath)
        (df, None, formatFile)
      case "JSON" =>
        val isJsonL = storageHandler.readAndExecute(new Path(dataPath))(isr => {
          val bufferedReader = new BufferedReader(isr)
          (Iterator continually bufferedReader.readLine takeWhile (_ != null))
            .map(_.trim)
            .filter(_.nonEmpty)
            .forall(line => {
              line.length >= 2 && line.startsWith("{") && line.endsWith("}")
            })
        })
        val df = session.read
          .option("multiLine", !isJsonL)
          .json(dataPath)
        (df, None, formatFile)
      case "XML" =>
        // find second occurrence of xml tag starting with letter in content
        val tag = {
          rowTag.getOrElse {
            storageHandler.readAndExecute(new Path(dataPath))(isr => {
              val bufferedReader = new BufferedReader(isr)
              import scala.jdk.CollectionConverters._
              val lineIterator = bufferedReader.lines().iterator().asScala
              var content = ""
              var tag = None: Option[String]
              while (tag.isEmpty && lineIterator.hasNext) {
                val currentLine = lineIterator.next().trim.replace("<?", "")
                content = content + currentLine
                val firstXmlTagStart = content.indexOf("<")
                val secondXmlTagStart = content.indexOf('<', firstXmlTagStart + 1)
                val closingTag = content.indexOf(">", secondXmlTagStart)
                if (firstXmlTagStart != -1 && secondXmlTagStart != -1 && closingTag != -1) {
                  tag = Some(content.substring(secondXmlTagStart + 1, closingTag).split(' ')(0))
                }
              }
              val result = tag match {
                case Some(tagFound) => tagFound
                case None =>
                  tableName
              }
              logger.info(s"Using rowTag: $result")
              result
            })
          }
        }

        val df = session.read
          .format("com.databricks.spark.xml")
          .option("rowTag", tag)
          .option("inferSchema", value = inferSchema)
          .load(dataPath)

        (df, Some(tag), formatFile)
      case "DSV" =>
        val df = session.read
          .format("com.databricks.spark.csv")
          .option("header", value = true)
          .option("inferSchema", value = inferSchema)
          .option("delimiter", getSeparator(new Path(dataPath).firstLine))
          .option("parserLib", "UNIVOCITY")
          .load(dataPath)
        (df, None, formatFile)
    }
  }

  private implicit class RichPath(filePath: Path) {
    def firstLine(implicit storageHandler: StorageHandler): String =
      storageHandler.readAndExecute(filePath)(isr => {
        val bufferedReader = new BufferedReader(isr)
        bufferedReader.readLine()
      })

    def lastLine(implicit storageHandler: StorageHandler): String =
      storageHandler.readAndExecute(filePath)(isr => {
        val bufferedReader = new BufferedReader(isr)
        (Iterator continually bufferedReader.readLine takeWhile (_ != null)).foldLeft("") {
          case (_, line) => line
        }
      })

    def lines(implicit storageHandler: StorageHandler): List[String] =
      storageHandler.readAndExecute(filePath)(isr => {
        val bufferedReader = new BufferedReader(isr)
        (Iterator continually bufferedReader.readLine takeWhile (_ != null)).toList
      })
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
    clean: Boolean,
    variant: Boolean = false
  )(implicit storageHandler: StorageHandler): Try[Path] = {
    Try {
      val path = new Path(inputPath)

      val schema = forceFormat match {
        case Some(Format.POSITION) =>
          val content =
            if (forceFormat.exists(Format.isBinary))
              List("")
            else {
              path.lines
            }
          val lines = content.map(_.trim).filter(_.nonEmpty)
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
            Attribute(
              name = fieldName,
              position = Some(Position(startPosition, endPosition)),
              sample = Option(field)
            )
          }
          val metadata = InferSchemaHandler.createMetaData(Format.POSITION)

          InferSchemaHandler.createSchema(
            tableName,
            Pattern.compile(pattern.getOrElse(InferSchemaJob.getSchemaPattern(path.getName))),
            comment,
            attributes,
            Some(metadata),
            None
          )

        case forceFormat =>
          val (dataframeWithFormat, xmlTag, formatStr) =
            createDataFrameWithFormat(
              inputPath,
              tableName,
              rowTag,
              forceFormat = forceFormat
            )
          val array = formatStr == "JSON_ARRAY"
          val format = Format.fromString(formatStr)
          val targetAttributeTypes =
            InferSchemaHandler.adjustAttributes(dataframeWithFormat.schema, format)(_)

          val dataframeToInfer = format match {
            case Format.DSV =>
              val (rawDataframeWithFormat, _, _) =
                createDataFrameWithFormat(
                  inputPath,
                  tableName,
                  rowTag,
                  inferSchema = false,
                  Some(format)
                )
              rawDataframeWithFormat
            case _ =>
              dataframeWithFormat
          }
          val adjustedAttributesType: Option[Row] = dataframeToInfer
            .transform(targetAttributeTypes)
            .collect()
            .headOption

          val adjustedAttributesTypeMap = adjustedAttributesType
            .map(r => {
              r.schema.fields.map { f =>
                if (f.name.endsWith(InferSchemaHandler.sampleColumnSuffix))
                  f.name -> r.getAs[String](f.name)
                else
                  f.name -> DataTypesToInt.typeIntToTypeStr(r.getAs[Int](f.name))
              }.toMap
            })
            .getOrElse(Map.empty)
          val attributes: List[Attribute] =
            InferSchemaHandler.createAttributes(
              adjustedAttributesTypeMap,
              dataframeWithFormat.schema
            )
          val preciseFormat =
            format match {
              case Format.JSON =>
                if (attributes.exists(_.attributes.nonEmpty)) Format.JSON else Format.JSON_FLAT
              case _ => format
            }
          val xmlOptions = xmlTag.map(tag => Map("rowTag" -> tag))
          val metadata = InferSchemaHandler.createMetaData(
            preciseFormat,
            Option(array),
            Some(true),
            format match {
              case DSV => Some(getSeparator(path.firstLine))
              case _   => None
            },
            xmlOptions
          )

          val strategy = WriteStrategy(
            `type` = Some(WriteStrategyType.fromWriteMode(writeMode))
          )
          val sample =
            metadata.resolveFormat() match {
              case Format.JSON | Format.JSON_FLAT =>
                if (metadata.resolveArray())
                  dataframeToInfer
                    .limit(20)
                    .toJSON
                    .collect()
                    .mkString("[", metadata.resolveSeparator(), "]")
                else
                  dataframeToInfer.limit(20).toJSON.collect().mkString("\n")
              case Format.DSV =>
                dataframeToInfer.limit(20).collect().mkString("\n")
              case _ =>
                dataframeToInfer.limit(20).toJSON.collect().mkString("\n")
            }
          InferSchemaHandler.createSchema(
            tableName,
            Pattern.compile(pattern.getOrElse(InferSchemaJob.getSchemaPattern(path.getName))),
            comment,
            attributes,
            Some(metadata.copy(writeStrategy = Some(strategy))),
            sample = Some(sample)
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

object InferSchemaJob {
  def getSchemaPattern(filename: String): String = {
    val parts = filename.split("\\.")
    if (parts.length < 2)
      filename
    else {
      // pattern extension
      val extension = parts.last

      // remove extension from filename hello-1234.json => hello-1234
      val prefix = filename.substring(0, filename.length - (extension.length + 1))

      val indexOfNonAlpha = prefix.lastIndexWhere(!_.isLetterOrDigit)
      val prefixWithoutNonAlpha =
        if (
          indexOfNonAlpha != -1 &&
          indexOfNonAlpha < prefix.length &&
          prefix(indexOfNonAlpha + 1).isDigit
        )
          prefix.substring(0, indexOfNonAlpha + 1) // hello-1234 => hello-
        else prefix
      if (prefixWithoutNonAlpha.isEmpty)
        filename
      else
        s"$prefixWithoutNonAlpha.*.$extension"
    }
  }
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    implicit val settings = Settings(config, None, None)

    val job = new InferSchemaJob()
    job.infer(
      domainName = "domain",
      tableName = "table",
      pattern = None,
      comment = None,
      // inputPath = "/Users/hayssams/Downloads/jsonarray.json",
      inputPath = "/Users/hayssams/Downloads/ndjson-sample.json",
      saveDir = "/Users/hayssams/tmp/aaa",
      forceFormat = None,
      writeMode = WriteMode.OVERWRITE,
      rowTag = None,
      clean = false
    )(settings.storageHandler())
  }
}
