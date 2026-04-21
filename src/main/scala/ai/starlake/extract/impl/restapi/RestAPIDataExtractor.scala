package ai.starlake.extract.impl.restapi

import ai.starlake.config.Settings
import ai.starlake.extract.ParUtils
import ai.starlake.schema.handlers.StorageHandler
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.csv.{CsvFormat, CsvWriter, CsvWriterSettings}
import org.apache.hadoop.fs.Path

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Configuration for REST API data extraction */
case class RestAPIDataExtractConfig(
  extractConfig: RestAPIExtractSchema,
  baseOutputDir: Path,
  limit: Int = 0,
  parallelism: Option[Int] = None,
  incremental: Boolean = false
)

/** Extracts data from REST API endpoints and writes to CSV files. Data is written to
  * {outputDir}/{domain}/{tableName}-{timestamp}.csv and can then be picked up by the standard
  * Starlake ingestion pipeline.
  */
class RestAPIDataExtractor(implicit settings: Settings) extends LazyLogging {

  private val stateMapper = new ObjectMapper()
  stateMapper.registerModule(DefaultScalaModule)

  private val extractionDateTime: String =
    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

  def run(config: RestAPIDataExtractConfig): Try[Long] = {
    val client = new RestAPIClient(config.extractConfig)
    val endpoints = config.extractConfig.resolvedEndpoints
    implicit val storageHandler: StorageHandler = settings.storageHandler()

    Try {
      val results = ParUtils.runInParallel(endpoints, config.parallelism) { endpoint =>
        extractEndpointData(client, endpoint, config, None)
      }
      results.sum
    }
  }

  private def extractEndpointData(
    client: RestAPIClient,
    endpoint: RestAPIEndpoint,
    config: RestAPIDataExtractConfig,
    parentRecord: Option[JsonNode]
  )(implicit storageHandler: StorageHandler): Long = {
    val resolvedPath = resolveParentPlaceholders(endpoint.path, parentRecord)
    val resolvedEndpoint = endpoint.copy(path = resolvedPath)
    val tableName = resolvedEndpoint.tableName
    val domainDir = new Path(config.baseOutputDir, resolvedEndpoint.domain)

    // Incremental: read last extracted value and add as query param
    val incrementalParams: Map[String, String] =
      if (config.incremental) {
        endpoint.incrementalField
          .flatMap { field =>
            readIncrementalState(config.baseOutputDir, resolvedEndpoint.domain, tableName)
              .map(lastValue => Map(field -> lastValue))
          }
          .getOrElse(Map.empty)
      } else Map.empty

    val endpointWithIncrParams = if (incrementalParams.nonEmpty) {
      resolvedEndpoint.copy(queryParams = resolvedEndpoint.queryParams ++ incrementalParams)
    } else resolvedEndpoint

    logger.info(s"Extracting data from ${endpointWithIncrParams.path} as $tableName")

    // Ensure output directory exists
    if (!storageHandler.exists(domainDir)) {
      storageHandler.mkdirs(domainDir)
    }

    val outputFile =
      new Path(domainDir, s"$tableName-$extractionDateTime.csv")

    var totalRecords = 0L
    var headerWritten = false
    var csvWriter: CsvWriter = null
    var outputWriter: OutputStreamWriter = null
    // Buffer parent records if children need processing (avoids re-fetching)
    val parentRecordBuffer =
      if (endpoint.children.nonEmpty) scala.collection.mutable.ListBuffer[JsonNode]()
      else null
    // Track max incremental field value
    var maxIncrementalValue: Option[String] = None
    val incrementalField = endpoint.incrementalField

    try {
      val pages = client.fetchAllPages(endpointWithIncrParams)

      for ((dataNode, _) <- pages) {
        val records = if (dataNode.isArray) {
          dataNode.elements().asScala.toList
        } else {
          List(dataNode)
        }

        if (records.nonEmpty) {
          // Lazily initialize writer on first data
          if (!headerWritten) {
            val headers = flattenFieldNames(records.head, "")
            outputWriter = new OutputStreamWriter(
              storageHandler.output(outputFile),
              StandardCharsets.UTF_8
            )
            val writerSettings = new CsvWriterSettings()
            val format = new CsvFormat()
            format.setDelimiter(',')
            writerSettings.setFormat(format)
            writerSettings.setHeaderWritingEnabled(true)
            writerSettings.setHeaders(headers: _*)
            writerSettings.setQuoteAllFields(true)
            csvWriter = new CsvWriter(outputWriter, writerSettings)
            csvWriter.writeHeaders()
            headerWritten = true
          }

          for (record <- records) {
            if (config.limit <= 0 || totalRecords < config.limit) {
              val row = flattenValues(record, "")
              csvWriter.writeRow(row: _*)
              totalRecords += 1
              // Buffer for children processing
              if (parentRecordBuffer != null) parentRecordBuffer += record
              // Track incremental field max value
              incrementalField.foreach { field =>
                val fieldNode = record.get(field)
                if (fieldNode != null && !fieldNode.isNull) {
                  val value = fieldNode.asText()
                  maxIncrementalValue = maxIncrementalValue match {
                    case None                       => Some(value)
                    case Some(prev) if value > prev => Some(value)
                    case existing                   => existing
                  }
                }
              }
            }
          }
        }
      }

      // Process children endpoints using buffered parent records
      if (parentRecordBuffer != null && parentRecordBuffer.nonEmpty) {
        for (parentRec <- parentRecordBuffer; child <- endpoint.children) {
          Try(extractEndpointData(client, child, config, Some(parentRec))) match {
            case Success(count) =>
              logger.info(
                s"Extracted $count child records from ${child.path} for parent ${endpointWithIncrParams.path}"
              )
            case Failure(e) =>
              logger.error(
                s"Failed to extract child ${child.path}: ${e.getMessage}"
              )
          }
        }
      }

      // Persist incremental state
      if (config.incremental) {
        maxIncrementalValue.foreach { value =>
          writeIncrementalState(
            config.baseOutputDir,
            endpointWithIncrParams.domain,
            tableName,
            value
          )
        }
      }

      logger.info(
        s"Extracted $totalRecords records from ${endpointWithIncrParams.path} into $outputFile"
      )
      totalRecords
    } finally {
      if (csvWriter != null) csvWriter.close()
      if (outputWriter != null) outputWriter.close()
    }
  }

  /** Replace {parent.fieldName} placeholders in the path with values from the parent record */
  private def resolveParentPlaceholders(path: String, parentRecord: Option[JsonNode]): String = {
    parentRecord match {
      case None => path
      case Some(parent) =>
        val pattern = """\{parent\.([^}]+)\}""".r
        pattern.replaceAllIn(
          path,
          m => {
            val field = m.group(1)
            val value = parent.get(field)
            if (value == null || value.isNull) {
              throw new RestAPIException(
                s"Parent record does not have field '$field' for path placeholder"
              )
            }
            java.net.URLEncoder.encode(value.asText(), "UTF-8")
          }
        )
    }
  }

  /** Flatten a JSON record's field names using dot notation for nested objects */
  private def flattenFieldNames(node: JsonNode, prefix: String): Array[String] = {
    node.fieldNames().asScala.toArray.flatMap { fieldName =>
      val fullName = if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"
      val fieldNode = node.get(fieldName)
      if (fieldNode != null && fieldNode.isObject) {
        flattenFieldNames(fieldNode, fullName)
      } else {
        Array(fullName)
      }
    }
  }

  /** Flatten a JSON record's values matching the order from flattenFieldNames */
  private def flattenValues(node: JsonNode, prefix: String): Array[String] = {
    node.fieldNames().asScala.toArray.flatMap { fieldName =>
      val fieldNode = node.get(fieldName)
      if (fieldNode != null && fieldNode.isObject) {
        flattenValues(fieldNode, if (prefix.isEmpty) fieldName else s"$prefix.$fieldName")
      } else {
        Array(nodeToString(fieldNode))
      }
    }
  }

  private def nodeToString(node: JsonNode): String = {
    if (node == null || node.isNull || node.isMissingNode) ""
    else if (node.isTextual) node.asText()
    else if (node.isArray || node.isObject) node.toString // JSON string for complex types
    else node.asText()
  }

  // --- Incremental state management ---

  private def stateFilePath(baseOutputDir: Path, domain: String, tableName: String): Path =
    new Path(new Path(baseOutputDir, s".state/$domain"), s"$tableName.json")

  private def readIncrementalState(
    baseOutputDir: Path,
    domain: String,
    tableName: String
  )(implicit storageHandler: StorageHandler): Option[String] = {
    val path = stateFilePath(baseOutputDir, domain, tableName)
    if (storageHandler.exists(path)) {
      try {
        val content = storageHandler.read(path)
        val node = stateMapper.readTree(content)
        Option(node.get("lastValue")).map(_.asText())
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to read incremental state from $path: ${e.getMessage}")
          None
      }
    } else None
  }

  private def writeIncrementalState(
    baseOutputDir: Path,
    domain: String,
    tableName: String,
    lastValue: String
  )(implicit storageHandler: StorageHandler): Unit = {
    val path = stateFilePath(baseOutputDir, domain, tableName)
    val stateDir = path.getParent
    if (!storageHandler.exists(stateDir)) {
      storageHandler.mkdirs(stateDir)
    }
    val state = Map("lastValue" -> lastValue, "extractedAt" -> extractionDateTime)
    val content = stateMapper.writeValueAsString(state)
    storageHandler.write(content, path)
    logger.info(s"Saved incremental state: $tableName lastValue=$lastValue")
  }
}
