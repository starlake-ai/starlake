package ai.starlake.schema.generator

import ai.starlake.schema.model._
import ConnectionType._
import org.apache.poi.ss.usermodel._

import java.io.File
import scala.util.Try

class XlsAutoJobReader(input: Input, policyInput: Option[Input]) extends XlsModel {

  private val workbook: Workbook = input match {
    case InputPath(s)  => WorkbookFactory.create(new File(s))
    case InputFile(in) => WorkbookFactory.create(in)
  }

  private lazy val policies = new XlsPolicyReader(policyInput.getOrElse(input)).policies

  lazy val autoTasksDesc: List[AutoTaskDesc] = {

    val sheetSchema = workbook.getSheet("schemas")
    val (rowsSchema, headerMapSchema) =
      getColsOrder(sheetSchema, allSchemaJobHeaders.map { case (name, _) => name })
    rowsSchema.flatMap { row =>
      val jobNameOpt: Option[String] =
        Option(row.getCell(headerMapSchema("_job"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val domainOpt: Option[String] =
        Option(row.getCell(headerMapSchema("_domain"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val schemaOpt =
        Option(row.getCell(headerMapSchema("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val writeOpt =
        Option(row.getCell(headerMapSchema("_write"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(WriteMode.fromString)
      val partitionOpt =
        Option(
          row.getCell(headerMapSchema("_partition"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        )
          .flatMap(formatter.formatCellValue)
      val policiesOpt =
        Option(row.getCell(headerMapSchema("_policy"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.split(",").map(_.trim))
      val commentOpt =
        Option(
          row.getCell(headerMapSchema("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        )
          .flatMap(formatter.formatCellValue)

      val databaseOpt =
        Option(
          row.getCell(headerMapSchema("_database"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        )
          .flatMap(formatter.formatCellValue)

      val clustering =
        Option(
          row.getCell(headerMapSchema("_clustering"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split(",").toSeq)

      val tags =
        Option(
          row.getCell(headerMapSchema("_tags"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split(",").toSet).getOrElse(Set.empty)

      val tablePolicies = policiesOpt
        .map { tablePolicies =>
          policies.filter(p => tablePolicies.contains(p.name))
        }
        .getOrElse(Nil)

      val (withoutPredicate, withPredicate) = tablePolicies
        .partition(_.predicate.toUpperCase() == "TRUE")

      val acl = withoutPredicate.map(rls =>
        AccessControlEntry("roles/bigquery.dataViewer", rls.grants, rls.name)
      )
      val rls = withPredicate

      val presqlOpt = Option(
        row.getCell(headerMapSchema("_presql"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue).map(_.split("###").toList)

      val postsqlOpt = Option(
        row.getCell(headerMapSchema("_postsql"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue).map(_.split("###").toList)

      val sinkTypeOpt = Option(
        row.getCell(headerMapSchema("_sink"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val connectionTypeOpt = Try(sinkTypeOpt.map(ConnectionType.fromString)).toOption.flatten

      val connectionRefOpt = Option(
        row.getCell(headerMapSchema("_connectionRef"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val sinkOptionsOpt = Option(
        row.getCell(headerMapSchema("_options"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)
        .map(
          _.split(",")
            .flatMap(kv =>
              kv.trim.split("=").toList match {
                case k :: v :: Nil => Some(k -> v)
                case _             => None
              }
            )
            .toMap
        )

      val formatOpt = Option(
        row.getCell(headerMapSchema("_format"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val extensionOpt = Option(
        row.getCell(headerMapSchema("_extension"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val coalesceOpt = Option(
        row.getCell(headerMapSchema("_coalesce"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val partitionColumns = partitionOpt.map(List(_)).getOrElse(Nil)
      val writeStrategy =
        if (partitionColumns.nonEmpty && writeOpt.contains(WriteMode.OVERWRITE))
          WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE_BY_PARTITION))
        else
          WriteStrategy(
            `type` = Some(WriteStrategyType.fromWriteMode(writeOpt.getOrElse(WriteMode.APPEND)))
          )

      val allSinks = AllSinks(
        connectionRef = connectionRefOpt,
        partition = partitionOpt.map(List(_)),
        clustering = clustering,
        requirePartitionFilter = partitionOpt match {
          case Some(_) => Some(true)
          case _       => None
        },
        format = formatOpt,
        extension = extensionOpt,
        coalesce = coalesceOpt.map(_.trim.toLowerCase match {
          case "true" => true
          case _      => false
        }),
        options = sinkOptionsOpt
      )

      val task =
        if (domainOpt.isEmpty) None
        else
          Some(
            AutoTaskDesc(
              name = jobNameOpt.getOrElse(throw new Exception("Job name is required in XLS")),
              sql = None,
              database = databaseOpt,
              domain = domainOpt.getOrElse(throw new Exception("Domain name is required in XLS")),
              table = schemaOpt.getOrElse(throw new Exception("table name is required in XLS")),
              write = writeOpt.orElse(Some(WriteMode.OVERWRITE)),
              presql = presqlOpt.getOrElse(Nil),
              postsql = postsqlOpt.getOrElse(Nil),
              sink = Some(
                connectionTypeOpt match {
                  case Some(BQ)     => BigQuerySink.fromAllSinks(allSinks).toAllSinks()
                  case Some(FS)     => FsSink.fromAllSinks(allSinks).toAllSinks()
                  case Some(JDBC)   => JdbcSink.fromAllSinks(allSinks).toAllSinks()
                  case Some(ES)     => EsSink.fromAllSinks(allSinks).toAllSinks()
                  case Some(KAFKA)  => KafkaSink.fromAllSinks(allSinks).toAllSinks()
                  case Some(GCPLOG) => GcpLogSink.fromAllSinks(allSinks).toAllSinks()
                  case _            => allSinks
                }
              ),
              rls = rls,
              acl = acl,
              comment = commentOpt,
              attributesDesc = {
                schemaOpt
                  .flatMap(schema => Option(workbook.getSheet(schema)))
                  .map(sheet => {
                    val (rowsAttributes, headerMapAttributes) =
                      getColsOrder(sheet, allAttributeJobHeaders.map { case (k, _) => k })
                    rowsAttributes.flatMap { row =>
                      val nameOpt =
                        Option(
                          row.getCell(
                            headerMapAttributes("_name"),
                            Row.MissingCellPolicy.RETURN_BLANK_AS_NULL
                          )
                        )
                          .flatMap(formatter.formatCellValue)
                      val descriptionOpt =
                        Option(
                          row.getCell(
                            headerMapAttributes("_description"),
                            Row.MissingCellPolicy.RETURN_BLANK_AS_NULL
                          )
                        )
                          .flatMap(formatter.formatCellValue)

                      (nameOpt, descriptionOpt) match {
                        case (Some(name), Some(description)) if description.trim.nonEmpty =>
                          Some(
                            AttributeDesc(
                              name,
                              comment = descriptionOpt.getOrElse("")
                            )
                          )
                        case _ => None
                      }
                    }.toList
                  })
              }.getOrElse(Nil),
              python = None,
              tags = tags,
              writeStrategy = writeStrategy,
              taskTimeoutMs = None
            )
          )
      task
    }
  }.toList
}
