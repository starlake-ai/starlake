package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQuerySparkWriter
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.WriteMode
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}
import com.google.cloud.bigquery.{Dataset, Table}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import java.sql.Timestamp
import scala.concurrent.duration.Duration
import scala.util.Try

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

object BigQueryFreshnessInfo extends StrictLogging {
  def freshness(
    config: BigQueryTablesConfig,
    schemaHandler: SchemaHandler
  )(implicit isettings: Settings): List[FreshnessStatus] = {
    val tables: List[(Dataset, List[Table])] =
      BigQueryTableInfo.extractTableInfos(config)
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
                    val errorStatus =
                      getFreshnessStatus(
                        schemaHandler.getDatabase(domain).getOrElse(""),
                        domain.finalName,
                        tableInfo,
                        table.finalName,
                        freshness.error,
                        "ERROR",
                        "TABLE"
                      )

                    errorStatus.orElse {
                      getFreshnessStatus(
                        schemaHandler.getDatabase(domain).getOrElse(""),
                        domain.finalName,
                        tableInfo,
                        table.finalName,
                        freshness.warn,
                        "WARN",
                        "TABLE"
                      )
                    }
                }
            }
          }
      }
    }
    val tasks = schemaHandler.tasks()
    val jobsFreshnessStatuses = tables.flatMap { case (dsInfo, tableInfos) =>
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
                  val errorStatus =
                    getFreshnessStatus(
                      task.database.getOrElse(isettings.appConfig.database),
                      task.domain,
                      tableInfo,
                      task.table,
                      freshness.error,
                      "ERROR",
                      "JOB"
                    )
                  errorStatus.orElse {
                    getFreshnessStatus(
                      task.database.getOrElse(isettings.appConfig.database),
                      task.domain,
                      tableInfo,
                      task.table,
                      freshness.warn,
                      "WARN",
                      "JOB"
                    )

                  }
              }
          }
      }
    }
    val statuses = tablesFreshnessStatuses ++ jobsFreshnessStatuses

    if (config.persist) {
      val job = new SparkJob {
        override def name: String = "BigQueryFreshnessInfo"

        override implicit def settings: Settings = isettings

        /** Just to force any job to implement its entry point using within the "run" method
          *
          * @return
          *   : Spark Dataframe for Spark Jobs None otherwise
          */
        override def run(): Try[JobResult] = Try {
          val dfDataset = session.createDataFrame(statuses)
          SparkJobResult(Option(dfDataset))
        }
      }

      val jobResult = job.run()
      jobResult match {
        case scala.util.Success(SparkJobResult(Some(dfDataset))) =>
          BigQuerySparkWriter.sinkInAudit(
            dfDataset,
            "freshness_info",
            Some("Information related to table freshness"),
            Some(BigQuerySchemaConverters.toBigQuerySchema(dfDataset.schema)),
            config.writeMode.getOrElse(WriteMode.APPEND)
          )
        case scala.util.Success(_) =>
          logger.warn("Could not extract BigQuery tables info")
        case scala.util.Failure(exception) =>
          throw new Exception("Could not extract BigQuery tables info", exception)
      }

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
              settings.appConfig.tenant
            )
          )
        else
          None
    }
  }

  def run(args: Array[String], schemaHandler: SchemaHandler): List[FreshnessStatus] = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    val config =
      BigQueryTablesConfig
        .parse(args)
        .getOrElse(throw new Exception("Could not parse arguments"))
    freshness(config, schemaHandler)
  }
}
