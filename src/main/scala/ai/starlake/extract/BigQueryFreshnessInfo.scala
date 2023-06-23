package ai.starlake.extract

import ai.starlake.config.{Settings, SparkEnv}
import ai.starlake.job.sink.bigquery.BigQuerySparkWriter
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.WriteMode
import com.google.cloud.bigquery.{Dataset, Table}
import com.typesafe.config.ConfigFactory

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.duration.Duration

case class FreshnessStatus(
  domain: String,
  table: String,
  lastModifiedTime: java.sql.Timestamp,
  timestamp: java.sql.Timestamp,
  duration: Long,
  warnOrError: String,
  database: String,
  tenant: String
)

object BigQueryFreshnessInfo {
  def freshness(
    config: BigQueryFreshnessConfig
  )(implicit settings: Settings): List[FreshnessStatus] = {
    val tables: List[(Dataset, List[Table])] =
      BigQueryTableInfo.extractTableInfos(config, config.tables)
    import settings.storageHandler
    val schemaHandler = new SchemaHandler(storageHandler)
    val domains = schemaHandler.domains()
    val tablesFreshnessStatuses = tables.flatMap { case (dsInfo, tableInfos) =>
      val domain = domains.find(_.finalName.equalsIgnoreCase(dsInfo.getDatasetId.getDataset))
      domain match {
        case None => Nil
        case Some(domain) =>
          tableInfos.flatMap { tableInfo =>
            val tableName = tableInfo.getTableId.getTable
            val table = domain.tables.find(_.finalName.equalsIgnoreCase(tableName))
            table match {
              case None => Nil
              case Some(table) =>
                val freshness =
                  table.metadata.flatMap(_.freshness).orElse(domain.metadata.flatMap(_.freshness))
                freshness match {
                  case None => Nil
                  case Some(freshness) =>
                    val warnStatus =
                      getFreshnessStatus(
                        schemaHandler.getDatabase(domain, table.finalName).getOrElse(""),
                        domain.finalName,
                        tableInfo,
                        table.finalName,
                        freshness.warn,
                        "WARN",
                        "TABLE"
                      )
                    val errorStatus =
                      getFreshnessStatus(
                        schemaHandler.getDatabase(domain, table.finalName).getOrElse(""),
                        domain.finalName,
                        tableInfo,
                        table.finalName,
                        freshness.error,
                        "ERROR",
                        "TABLE"
                      )
                    List(warnStatus, errorStatus).flatten
                }
            }
          }
      }
    }
    val jobs = schemaHandler.jobs()
    val jobsFreshnessStatuses = tables.flatMap { case (dsInfo, tableInfos) =>
      val tasks = jobs.flatMap(_._2.tasks)
      val task = tasks
        .find(_.domain.equalsIgnoreCase(dsInfo.getDatasetId.getDataset))
      task match {
        case None => Nil
        case Some(task) =>
          val tableInfo = tableInfos.find(_.getTableId.getTable.equalsIgnoreCase(task.table))
          tableInfo match {
            case None => Nil
            case Some(tableInfo) =>
              val freshness = task.freshness
              freshness match {
                case None => Nil
                case Some(freshness) =>
                  val warnStatus =
                    getFreshnessStatus(
                      task.database.getOrElse(settings.comet.database),
                      task.domain,
                      tableInfo,
                      task.table,
                      freshness.warn,
                      "WARN",
                      "JOB"
                    )
                  val errorStatus =
                    getFreshnessStatus(
                      task.database.getOrElse(settings.comet.database),
                      task.domain,
                      tableInfo,
                      task.table,
                      freshness.error,
                      "ERROR",
                      "JOB"
                    )
                  List(warnStatus, errorStatus).flatten
              }
          }
      }
    }
    val statuses = tablesFreshnessStatuses ++ jobsFreshnessStatuses

    if (config.persist) {
      val session = new SparkEnv("BigQueryFreshnessInfo-" + UUID.randomUUID().toString).session
      val dfDataset = session.createDataFrame(statuses)
      BigQuerySparkWriter.sink(
        config.authInfo(),
        dfDataset,
        "freshness_info",
        Some("Information related to table freshness"),
        config.writeMode.getOrElse(WriteMode.OVERWRITE)
      )
    }
    statuses
  }

  private def getFreshnessStatus(
    domainDatabaseName: String,
    domainName: String,
    tableInfo: Table,
    tableName: String,
    duration: Option[String],
    level: String,
    typ: String
  )(implicit settings: Settings): Option[FreshnessStatus] = {
    duration match {
      case None => None
      case Some(duration) =>
        val warnOrErrorDuration = Duration(duration).toMillis
        val now = System.currentTimeMillis()
        val lastModifiedTime = tableInfo.getLastModifiedTime
        if (now - warnOrErrorDuration > lastModifiedTime)
          Some(
            FreshnessStatus(
              domainName,
              tableName,
              new Timestamp(lastModifiedTime),
              new Timestamp(now),
              warnOrErrorDuration,
              level,
              domainDatabaseName,
              settings.comet.tenant
            )
          )
        else
          None
    }
  }

  def run(args: Array[String]): List[FreshnessStatus] = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    val config =
      BigQueryFreshnessConfig
        .parse(args)
        .getOrElse(throw new Exception("Could not parse arguments"))
    freshness(config)
  }
}
