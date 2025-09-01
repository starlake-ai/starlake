package ai.starlake.extract.freshness

import ai.starlake.config.Settings
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.extract.{BigQueryTableInfo, JdbcTableInfo, TablesExtractConfig}
import ai.starlake.job.sink.bigquery.BigQuerySparkWriter
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Engine, WriteMode}
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}
import com.typesafe.scalalogging.LazyLogging

import java.sql.Timestamp
import scala.annotation.nowarn
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

object FreshnessJob extends LazyLogging {
  def freshness(
    config: TablesExtractConfig,
    schemaHandler: SchemaHandler
  )(implicit mySettings: Settings): List[FreshnessStatus] = {
    val conn = ConnectionInfo.getConnectionOrDefault(config.connectionRef)
    val tables: List[(String, List[(String, Long)])] = {
      conn.getJdbcEngineName() match {
        case Engine.BQ =>
          BigQueryTableInfo.extractLastModifiedTime(config)
        case Engine.SNOWFLAKE =>
          new JdbcTableInfo().extractLastModifiedTime(config)
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported connection type: ${conn.getJdbcEngineName()}. Only 'bigquery' is supported."
          )
      }

    }
    val domains = schemaHandler.domains()
    val tablesFreshnessStatuses = tables.flatMap { case (dsInfo, tableInfos) =>
      val domain = domains.find(_.finalName.equalsIgnoreCase(dsInfo))
      domain match {
        case None => Nil
        case Some(domain) =>
          tableInfos.flatMap { case (tableName, lastModifiedTime) =>
            val table = domain.tables.find(_.finalName.equalsIgnoreCase(tableName))
            table match {
              case None => None
              case Some(table) =>
                val freshness =
                  table.metadata.flatMap(_.freshness).orElse(domain.metadata.flatMap(_.freshness))
                freshness match {
                  case None =>
                    Some(
                      FreshnessStatus(
                        domain.finalName,
                        table.finalName,
                        new Timestamp(lastModifiedTime),
                        new Timestamp(System.currentTimeMillis()),
                        0L,
                        "INFO",
                        schemaHandler.getDatabase(domain).getOrElse(""),
                        mySettings.appConfig.tenant
                      )
                    )

                  case Some(freshness) =>
                    val errorStatus =
                      getFreshnessStatus(
                        schemaHandler.getDatabase(domain).getOrElse(""),
                        domain.finalName,
                        table.finalName,
                        lastModifiedTime,
                        freshness.error,
                        "ERROR",
                        "TABLE"
                      )

                    errorStatus
                      .orElse {
                        getFreshnessStatus(
                          schemaHandler.getDatabase(domain).getOrElse(""),
                          domain.finalName,
                          table.finalName,
                          lastModifiedTime,
                          freshness.warn,
                          "WARN",
                          "TABLE"
                        )
                      }
                      .orElse(
                        Some(
                          FreshnessStatus(
                            domain.finalName,
                            table.finalName,
                            new Timestamp(lastModifiedTime),
                            new Timestamp(System.currentTimeMillis()),
                            0L,
                            "INFO",
                            schemaHandler.getDatabase(domain).getOrElse(""),
                            mySettings.appConfig.tenant
                          )
                        )
                      )
                }
            }
          }
      }
    }
    val tasks = schemaHandler.tasks()
    val jobsFreshnessStatuses = tables.flatMap { case (dsInfo, tableInfos) =>
      val task = tasks
        .find(_.domain.equalsIgnoreCase(dsInfo))
      task match {
        case None => Nil
        case Some(task) =>
          val tableInfo = tableInfos.find(_._1.equalsIgnoreCase(task.table))
          tableInfo match {
            case None =>
              None
            case Some((tableName, lastModifiedTime)) =>
              val freshness = task.freshness
              freshness match {
                case None =>
                  Some(
                    FreshnessStatus(
                      task.domain,
                      task.table,
                      new Timestamp(lastModifiedTime),
                      new Timestamp(System.currentTimeMillis()),
                      0L,
                      "INFO",
                      task.database.getOrElse(mySettings.appConfig.database),
                      mySettings.appConfig.tenant
                    )
                  )

                case Some(freshness) =>
                  val errorStatus =
                    getFreshnessStatus(
                      task.database.getOrElse(mySettings.appConfig.database),
                      task.domain,
                      task.table,
                      lastModifiedTime,
                      freshness.error,
                      "ERROR",
                      "TASK"
                    )
                  errorStatus
                    .orElse {
                      getFreshnessStatus(
                        task.database.getOrElse(mySettings.appConfig.database),
                        task.domain,
                        task.table,
                        lastModifiedTime,
                        freshness.warn,
                        "WARN",
                        "TASK"
                      )
                    }
                    .orElse(
                      Some(
                        FreshnessStatus(
                          task.domain,
                          task.table,
                          new Timestamp(lastModifiedTime),
                          new Timestamp(System.currentTimeMillis()),
                          0L,
                          "INFO",
                          task.database.getOrElse(mySettings.appConfig.database),
                          mySettings.appConfig.tenant
                        )
                      )
                    )
              }
          }
      }
    }
    val statuses = tablesFreshnessStatuses ++ jobsFreshnessStatuses

    if (config.persist) {
      val job = new SparkJob {
        override def name: String = "BigQueryFreshnessInfo"

        override implicit def settings: Settings = settings

        /** Just to force any job to implement its entry point using within the "run" method
          *
          * @return
          *   : Spark Dataframe for Spark Jobs None otherwise
          */
        override def run(): Try[JobResult] = Try {
          val dfDataset = session.createDataFrame(statuses)
          SparkJobResult(Option(dfDataset), None)
        }
      }

      val jobResult = job.run()
      jobResult match {
        case scala.util.Success(SparkJobResult(Some(dfDataset), _)) =>
          BigQuerySparkWriter.sinkInAudit(
            dfDataset,
            "freshness_info",
            Some("Information related to table freshness"),
            Some(BigQuerySchemaConverters.toBigQuerySchema(dfDataset.schema)),
            config.writeMode.getOrElse(WriteMode.APPEND),
            accessToken = config.accessToken
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
    tableName: String,
    lastModifiedTime: Long,
    duration: Option[String],
    level: String,
    typ: String
  )(implicit settings: Settings): Option[FreshnessStatus] = {
    duration match {
      case None => None
      case Some(duration) =>
        val warnOrErrorDuration = Duration(duration).toMillis
        val now = System.currentTimeMillis()
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

  @nowarn
  def run(args: Array[String], schemaHandler: SchemaHandler): Try[Unit] = {
    FreshnessExtractCmd.parse(args) match {
      case Some(config) =>
        implicit val settings: Settings = Settings(Settings.referenceConfig, None, None, None)
        FreshnessExtractCmd.run(config, schemaHandler).map(_ => ())
      case None =>
        Try(throw new IllegalArgumentException(FreshnessExtractCmd.usage()))
    }
  }
}
