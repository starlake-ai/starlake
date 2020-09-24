package com.ebiznext.comet.schema.generator

import java.io.File
import java.util.regex.Pattern

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model._
import org.apache.poi.ss.usermodel._

import scala.collection.JavaConverters._

/**
  * Reads the spreadsheet found at the specified {@param path} and builds the corresponding Domain object
  * @param path
  */
class XlsReader(path: String) {

  private val workbook: Workbook = WorkbookFactory.create(new File(path))

  object formatter {
    private val f = new DataFormatter()

    def formatCellValue(cell: Cell): Option[String] = {
      f.formatCellValue(cell).trim match {
        case v if v.isEmpty => None
        case v              => Some(v)
      }
    }
  }

  private lazy val domain: Option[Domain] = {
    workbook.getSheet("domain").asScala.drop(1).headOption.flatMap { row =>
      val nameOpt = Option(row.getCell(0, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
      val directoryOpt = Option(row.getCell(1, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
      // Here for ack, we do not want to get None returned for an empty cell since None would give us a ".ack" as default later on
      val ack = Option(row.getCell(2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK))
        .flatMap(formatter.formatCellValue)
        .orElse(Some(""))
      val comment = Option(row.getCell(3, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
      (nameOpt, directoryOpt) match {
        case (Some(name), Some(directory)) =>
          Some(Domain(name, directory, ack = ack, comment = comment))
        case _ => None
      }
    }
  }

  private val allSchemaHeaders = List(
    "_name",
    "_pattern",
    "_mode",
    "_write",
    "_format",
    "_header",
    "_delimiter",
    "_delta_column",
    "_merge_keys",
    "_description",
    "_encoding",
    "_sampling",
    "_partitioning",
    "_sink",
    "_clustering"
  )

  private def getSchemaColOrder(sheet: Sheet): (Iterable[Row], Map[String, Int]) = {
    val scalaSheet = sheet.asScala
    val hasSchema = scalaSheet.head
      .getCell(0, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      .getStringCellValue
      .startsWith("_")
    if (hasSchema) {
      val headersRow = scalaSheet.head
      val headerMap = headersRow
        .cellIterator()
        .asScala
        .zipWithIndex
        .map { case (headerCell, index) =>
          val header = headerCell.getStringCellValue
          (header, index)
        }
        .toMap
      (scalaSheet.drop(2), headerMap)
    } else {
      (scalaSheet.drop(1), allSchemaHeaders.zipWithIndex.toMap)
    }
  }

  private lazy val schemas: List[Schema] = {
    val sheet = workbook.getSheet("schemas")
    val (rows, headerMap) = getSchemaColOrder(sheet)
    rows.flatMap { row =>
      val nameOpt =
        Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val patternOpt =
        Option(row.getCell(headerMap("_pattern"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(Pattern.compile)
      val mode: Option[Mode] =
        Option(row.getCell(headerMap("_mode"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(Mode.fromString)
      val write =
        Option(row.getCell(headerMap("_write"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(WriteMode.fromString)
      val format =
        Option(row.getCell(headerMap("_format"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(Format.fromString)
      val withHeader =
        Option(row.getCell(headerMap("_header"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.toBoolean)
      val separator =
        Option(row.getCell(headerMap("_delimiter"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val deltaColOpt =
        Option(row.getCell(headerMap("_delta_column"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val identityKeysOpt =
        Option(row.getCell(headerMap("_merge_keys"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val comment =
        Option(row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val encodingOpt =
        Option(row.getCell(headerMap("_encoding"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val partitionSamplingOpt =
        Option(row.getCell(headerMap("_sampling"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.toDouble)
      val partitionColumnsOpt =
        Option(row.getCell(headerMap("_partitioning"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.split(",") map (_.trim))
          .map(_.toList)
      val sinkColumnsOpt =
        Option(row.getCell(headerMap("_sink"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val clusteringOpt =
        Option(row.getCell(headerMap("_clustering"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue).map(_.split(","))

      (nameOpt, patternOpt) match {
        case (Some(name), Some(pattern)) => {
          val metaData = Metadata(
            mode,
            format,
            encoding = encodingOpt,
            multiline = None,
            array = None,
            withHeader,
            separator,
            write = write,
            partition = (partitionSamplingOpt, partitionColumnsOpt) match {
              case (None, None) => None
              case _ =>
                Some(
                  Partition(
                    sampling = partitionSamplingOpt,
                    attributes = partitionColumnsOpt
                  )
                )
            },
            sink = sinkColumnsOpt.map(Sink.fromType).map {
                case bqSink: BigQuerySink =>
                  val partitionBqSink = partitionColumnsOpt match {
                    case Some(ts :: Nil) => bqSink.copy(timestamp = Some(ts))
                    case _               => bqSink
                  }
                  val clusteredBqSink = clusteringOpt match {
                    case Some(cluster) =>
                      partitionBqSink.copy(clustering = Some(cluster))
                    case _ => partitionBqSink
                  }
                  clusteredBqSink
                case sink =>
                  sink
              },
            clustering = clusteringOpt match {
              case Some(cluster) => Some(cluster)
              case None => None
            }
          )

          val mergeOptions: Option[MergeOptions] = (deltaColOpt, identityKeysOpt) match {
            case (Some(deltaCol), Some(identityKeys)) =>
              Some(
                MergeOptions(
                  key = identityKeys.split(",").toList.map(_.trim),
                  timestamp = Some(deltaCol)
                )
              )
            case (None, Some(identityKeys)) =>
              Some(
                MergeOptions(key = identityKeys.split(",").toList.map(_.trim))
              )
            case _ => None
          }
          Some(
            Schema(
              name,
              pattern,
              attributes = Nil,
              Some(metaData),
              mergeOptions,
              comment,
              None,
              None
            )
          )
        }
        case _ => None
      }
    }.toList
  }

  private def buildSchemas(settings: Settings): List[Schema] = {
    schemas.map { schema =>
      val schemaName = schema.name
      val sheetOpt = Option(workbook.getSheet(schemaName))
      val attributes = sheetOpt match {
        case None => List.empty
        case Some(sheet) =>
          val scalaSheet = sheet.asScala
          scalaSheet
            .drop(1)
            .flatMap { row =>
              val nameOpt = Option(row.getCell(0, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
              val renameOpt = Option(row.getCell(1, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
              val semTypeOpt = Option(row.getCell(2, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
              val required = Option(row.getCell(3, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
                .forall(_.toBoolean)
              val privacy = Option(row.getCell(4, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
                .map(PrivacyLevel.ForSettings(settings).fromString)
              val metricType = Option(row.getCell(5, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
                .map(MetricType.fromString)
              val defaultOpt = Option(row.getCell(6, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
              val scriptOpt = Option(row.getCell(7, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
              val commentOpt = Option(row.getCell(8, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)

              val positionOpt = schema.metadata.flatMap(_.format) match {
                case Some(Format.POSITION) => {
                  val positionStart =
                    Option(row.getCell(9, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                      .flatMap(formatter.formatCellValue)
                      .map(_.toInt) match {
                      case Some(v) => v - 1
                      case _       => 0
                    }
                  val positionEnd =
                    Option(row.getCell(10, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                      .flatMap(formatter.formatCellValue)
                      .map(_.toInt) match {
                      case Some(v) => v - 1
                      case _       => 0
                    }
                  Some(Position(positionStart, positionEnd))
                }
                case _ => None
              }
              val attributeTrim =
                Option(row.getCell(11, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                  .flatMap(formatter.formatCellValue)
                  .map(Trim.fromString)

              (nameOpt, semTypeOpt) match {
                case (Some(name), Some(semType)) =>
                  Some(
                    Attribute(
                      name,
                      semType,
                      array = None,
                      required,
                      privacy,
                      comment = commentOpt,
                      rename = renameOpt,
                      metricType = metricType,
                      trim = attributeTrim,
                      position = positionOpt,
                      default = defaultOpt,
                      script = scriptOpt,
                      tags = None,
                      attributes = None
                    )
                  )
                case _ => None
              }
            }
            .toList
      }
      schema.copy(attributes = attributes)
    }
  }

  /**
    * Returns the Domain corresponding to the parsed spreadsheet
    * @param settings
    * @return an Option of Domain
    */
  def getDomain()(implicit settings: Settings): Option[Domain] = {
    val completeSchemas = buildSchemas(settings).filter(_.attributes.nonEmpty)
    domain.map(_.copy(schemas = completeSchemas))
  }
}
