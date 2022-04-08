package ai.starlake.schema.generator

import ai.starlake.config.{PrivacyLevels, Settings}
import ai.starlake.privacy.PrivacyEngine
import ai.starlake.schema.model._
import org.apache.poi.ss.usermodel._

import java.io.File
import java.util.regex.Pattern
import scala.collection.JavaConverters._

sealed trait Input

case class Path(path: String) extends Input

case class FileInput(file: File) extends Input

final case class SchemaName(value: String) extends AnyVal

/** Reads the spreadsheet found at the specified {@param input} and builds the corresponding Domain
  * object
  *
  * @param input
  */
class XlsReader(input: Input) extends XlsModel {

  private val workbook: Workbook = input match {
    case Path(s)       => WorkbookFactory.create(new File(s))
    case FileInput(in) => WorkbookFactory.create(in)
  }

  private lazy val domain: Option[Domain] = {
    val sheet = workbook.getSheet("domain")
    val (rows, headerMap) = getColsOrder(sheet, allDomainHeaders.map { case (k, _) => k })
    rows.headOption.flatMap { row =>
      val nameOpt =
        Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val renameOpt =
        Option(row.getCell(headerMap("_rename"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val directoryOpt =
        Option(row.getCell(headerMap("_path"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      // Here for ack, we do not want to get None returned for an empty cell since None would give us a ".ack" as default later on
      val ack = Option(row.getCell(headerMap("_ack"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK))
        .flatMap(formatter.formatCellValue)
        .orElse(Some(""))
      val comment =
        Option(row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val schemaRefsOpt =
        Option(row.getCell(headerMap("_schema_refs"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.split(",").toList)
      (nameOpt, directoryOpt) match {
        case (Some(name), Some(directory)) =>
          Some(
            Domain(
              name,
              metadata = Some(Metadata(directory = Some(directory), ack = ack)),
              comment = comment,
              tableRefs = schemaRefsOpt,
              rename = renameOpt
            )
          )
        case _ => None
      }
    }
  }

  private lazy val schemas: List[(Schema, SchemaName)] = {
    val sheet = workbook.getSheet("schemas")
    val (rows, headerMap) = getColsOrder(sheet, allSchemaHeaders.map { case (name, _) => name })
    rows.flatMap { row =>
      val nameOpt =
        Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val renameOpt =
        Option(row.getCell(headerMap("_rename"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
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
          .flatMap(formatter.formatCellValue)
          .map(_.split(","))
      val mergeQueryFilter =
        Option(
          row.getCell(headerMap("_merge_query_filter"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue)
      val presql =
        Option(
          row.getCell(headerMap("_presql"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split("###").toList)
      val postsql =
        Option(
          row.getCell(headerMap("_postsql"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split("###").toList)

      val primaryKeys =
        Option(
          row.getCell(headerMap("_primary_key"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split(",").toList)

      val tags =
        Option(
          row.getCell(headerMap("_tags"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split(",").toSet)

      val longNameOpt =
        Option(row.getCell(headerMap("_long_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)

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
              case None          => None
            }
          )

          val mergeOptions: Option[MergeOptions] =
            (deltaColOpt, identityKeysOpt, mergeQueryFilter) match {
              case (Some(deltaCol), Some(identityKeys), Some(filter)) =>
                Some(
                  MergeOptions(
                    key = identityKeys.split(",").toList.map(_.trim),
                    timestamp = Some(deltaCol),
                    queryFilter = Some(filter)
                  )
                )
              case (Some(deltaCol), Some(identityKeys), _) =>
                Some(
                  MergeOptions(
                    key = identityKeys.split(",").toList.map(_.trim),
                    timestamp = Some(deltaCol)
                  )
                )
              case (None, Some(identityKeys), Some(filter)) =>
                Some(
                  MergeOptions(
                    key = identityKeys.split(",").toList.map(_.trim),
                    queryFilter = Some(filter)
                  )
                )
              case (None, Some(identityKeys), _) =>
                Some(
                  MergeOptions(key = identityKeys.split(",").toList.map(_.trim))
                )
              case _ => None
            }

          val schema = Schema(
            name = longNameOpt.getOrElse(name),
            pattern = pattern,
            attributes = Nil,
            metadata = Some(metaData),
            merge = mergeOptions,
            comment = comment,
            presql = presql,
            postsql = postsql,
            tags = tags,
            primaryKey = primaryKeys,
            rename = renameOpt
          )
          Some(schema, SchemaName(name))
        }
        case _ => None
      }
    }.toList
  }

  /** Returns the Domain corresponding to the parsed spreadsheet
    * @param settings
    * @return
    *   an Option of Domain
    */
  def getDomain()(implicit settings: Settings): Option[Domain] = {
    val completeSchemas = buildSchemas(settings).filter(_.attributes.nonEmpty)
    domain.map(_.copy(tables = completeSchemas))
  }

  private def buildSchemas(settings: Settings): List[Schema] = {
    schemas.map { case (schema, originalName) =>
      val schemaName = originalName.value
      val sheetOpt = Option(workbook.getSheet(schemaName))
      val attributes = sheetOpt match {
        case None => List.empty
        case Some(sheet) =>
          val (rows, headerMap) = getColsOrder(sheet, allAttributeHeaders.map { case (k, _) => k })
          rows.flatMap { row =>
            val nameOpt =
              Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
            val renameOpt =
              Option(row.getCell(headerMap("_rename"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
            val semTypeOpt =
              Option(row.getCell(headerMap("_type"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
            val required = Option(
              row.getCell(headerMap("_required"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
            ).flatMap(formatter.formatCellValue)
              .forall(_.toBoolean)
            val privacy =
              Option(row.getCell(headerMap("_privacy"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
                .map { value =>
                  val allPrivacyLevels =
                    PrivacyLevels.allPrivacyLevels(settings.comet.privacy.options)
                  val ignore: Option[((PrivacyEngine, List[String]), PrivacyLevel)] =
                    allPrivacyLevels.get(value.toUpperCase)
                  ignore.map { case (_, level) => level }.getOrElse {
                    if (value.toUpperCase().startsWith("SQL:"))
                      PrivacyLevel(value.substring("SQL:".length), true)
                    else
                      throw new Exception(s"key not found: $value")
                  }
                }
            val metricType =
              Option(row.getCell(headerMap("_metric"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
                .map(MetricType.fromString)
            val defaultOpt =
              Option(row.getCell(headerMap("_default"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
            val scriptOpt =
              Option(row.getCell(headerMap("_script"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
            val commentOpt = Option(
              row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
            ).flatMap(formatter.formatCellValue)

            val positionOpt = schema.metadata.flatMap(_.format) match {
              case Some(Format.POSITION) => {
                val positionStart =
                  Option(
                    row.getCell(
                      headerMap("_position_start"),
                      Row.MissingCellPolicy.RETURN_BLANK_AS_NULL
                    )
                  ).flatMap(formatter.formatCellValue)
                    .map(_.toInt) match {
                    case Some(v) => v - 1
                    case _       => 0
                  }
                val positionEnd =
                  Option(
                    row.getCell(
                      headerMap("_position_end"),
                      Row.MissingCellPolicy.RETURN_BLANK_AS_NULL
                    )
                  ).flatMap(formatter.formatCellValue)
                    .map(_.toInt) match {
                    case Some(v) => v - 1
                    case _       => 0
                  }
                Some(Position(positionStart, positionEnd))
              }
              case _ => None
            }
            val attributeTrim =
              Option(row.getCell(headerMap("_trim"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
                .map(Trim.fromString)

            val attributeIgnore =
              Option(row.getCell(headerMap("_ignore"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                .flatMap(formatter.formatCellValue)
                .map(_.toBoolean)

            val foreignKey =
              Option(
                row.getCell(headerMap("_foreign_key"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
              ).flatMap(formatter.formatCellValue)

            val tags =
              Option(
                row.getCell(headerMap("_tags"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
              ).flatMap(formatter.formatCellValue).map(_.split(",").toSet)

            (nameOpt, semTypeOpt) match {
              case (Some(name), Some(semanticType)) =>
                Some(
                  Attribute(
                    name = name,
                    `type` = semanticType,
                    array = None,
                    required = required,
                    privacy.getOrElse(PrivacyLevel.None),
                    comment = commentOpt,
                    rename = renameOpt,
                    metricType = metricType,
                    trim = attributeTrim,
                    position = positionOpt,
                    default = defaultOpt,
                    script = scriptOpt,
                    attributes = None,
                    ignore = attributeIgnore,
                    foreignKey = foreignKey,
                    tags = tags
                  )
                )
              case _ => None
            }
          }.toList
      }
      schema.copy(attributes = attributes)
    }
  }

  private def getColsOrder(
    sheet: Sheet,
    allHeaders: List[String]
  ): (Iterable[Row], Map[String, Int]) = {
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
      (scalaSheet.drop(1), allHeaders.zipWithIndex.toMap)
    }
  }

  object formatter {
    private val f = new DataFormatter()

    def formatCellValue(cell: Cell): Option[String] = {
      // remove all no-breaking spaces from cell to avoid parsing errors
      f.formatCellValue(cell).trim.replaceAll("\\u00A0", "") match {
        case v if v.isEmpty => None
        case v              => Some(v)
      }
    }
  }
}
