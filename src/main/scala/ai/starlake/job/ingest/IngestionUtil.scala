package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobResult
import ai.starlake.job.transform.SparkAutoTask
import ai.starlake.job.validator.SimpleRejectedRecord
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.GcpUtils
import com.google.cloud.bigquery.LegacySQLTypeName
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}
object IngestionUtil {

  private val rejectedCols = List(
    ("jobid", LegacySQLTypeName.STRING, StringType),
    ("timestamp", LegacySQLTypeName.TIMESTAMP, TimestampType),
    ("domain", LegacySQLTypeName.STRING, StringType),
    ("schema", LegacySQLTypeName.STRING, StringType),
    ("error", LegacySQLTypeName.STRING, StringType),
    ("path", LegacySQLTypeName.STRING, StringType)
  )

  def sinkRejected(
    applicationId: String,
    session: SparkSession,
    rejectedDS: Dataset[SimpleRejectedRecord],
    domainName: String,
    schemaName: String,
    now: Timestamp,
    paths: List[Path],
    scheduledDate: Option[String]
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[(Dataset[Row], Path)] = {
    import session.implicits._
    val rejectedPathName = paths.map(_.toString).mkString(",")
    // We need to save first the application ID
    // referencing it inside the worker (ds.map) below would fail.
    val rejectedTypedDS = rejectedDS.map { err =>
      RejectedRecord(
        applicationId,
        now,
        domainName,
        schemaName,
        err.errors, // TODO: Wondering if we should remove file name in errors and rely on err.path for rejectedPathName instead
        rejectedPathName
      )
    }
    val limitedRejectedTypedDS = rejectedTypedDS.limit(settings.appConfig.audit.maxErrors)
    val rejectedDF =
      limitedRejectedTypedDS.toDF(rejectedCols.map { case (attrName, _, _) => attrName }: _*)

    val auditSink = settings.appConfig.audit.getSink()
    auditSink.getConnectionType() match {
      case ConnectionType.GCPLOG =>
        val logName = settings.appConfig.audit.getDomainRejected()
        GcpUtils.sinkToGcpCloudLogging(
          limitedRejectedTypedDS.collect().map(_.asMap()),
          "rejected",
          logName
        )
        Success(rejectedDF, paths.head)

      case _ =>
        val taskDesc =
          AutoTaskInfo(
            name = s"rejected-$applicationId-$domainName-$schemaName",
            sql = None,
            database = settings.appConfig.audit.getDatabase(),
            domain = settings.appConfig.audit.getDomain(),
            table = "rejected",
            presql = Nil,
            postsql = Nil,
            sink = Some(settings.appConfig.audit.sink),
            _auditTableName = Some("rejected"),
            connectionRef = settings.appConfig.audit.sink.connectionRef
          )

        val autoTask = new SparkAutoTask(
          appId = Option(applicationId),
          taskDesc = taskDesc,
          commandParameters = Map.empty,
          interactive = None,
          truncate = false,
          test = false,
          logExecution = false,
          resultPageSize = 200,
          resultPageNumber = 1,
          scheduledDate = scheduledDate
        )
        val res = autoTask.sink(rejectedDF)
        if (res) {
          Success(rejectedDF, paths.head)
        } else {
          Failure(new Exception("Failed to save rejected"))
        }
    }
  }
}

case class BqLoadInfo(
  totalAcceptedRows: Long,
  totalRejectedRows: Long,
  paths: List[String],
  jobResult: BigQueryJobResult
) {
  val totalRows: Long = totalAcceptedRows + totalRejectedRows
}
