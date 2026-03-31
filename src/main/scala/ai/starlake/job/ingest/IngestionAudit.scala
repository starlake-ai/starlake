package ai.starlake.job.ingest

import ai.starlake.utils.{IngestionCounters, Utils}

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Try}

/** Audit logging concerns for ingestion jobs. */
trait IngestionAudit { self: IngestionJob =>

  private def buildAuditLog(
    paths: String,
    success: Boolean,
    inputCount: Long,
    acceptedCount: Long,
    rejectedCount: Long,
    start: Timestamp,
    duration: Long,
    message: String
  ): AuditLog = AuditLog(
    applicationId(),
    Some(paths),
    domain.name,
    schema.name,
    success,
    inputCount,
    acceptedCount,
    rejectedCount,
    start,
    duration,
    message,
    Step.LOAD.toString,
    schemaHandler.getDatabase(domain),
    settings.appConfig.tenant,
    test = false,
    scheduledDate
  )

  def logLoadFailureInAudit(start: Timestamp, exception: Throwable): Failure[Nothing] = {
    exception.printStackTrace()
    val end = Timestamp.from(Instant.now())
    val err = Utils.exceptionAsString(exception)
    val duration = end.getTime - start.getTime
    val logs = if (settings.appConfig.audit.detailedLoadAudit) {
      path.map { p =>
        buildAuditLog(p.toString, success = false, 0, 0, 0, start, duration, err)
      }
    } else {
      List(
        buildAuditLog(
          path.map(_.toString).mkString(","),
          success = false,
          0,
          0,
          0,
          start,
          duration,
          err
        )
      )
    }
    AuditLog.sink(logs, accessToken)(settings, storageHandler, schemaHandler)
    logger.error(err)
    Failure(exception)
  }

  def logLoadInAudit(
    start: Timestamp,
    ingestionCounters: List[IngestionCounters]
  ): Try[List[AuditLog]] = {
    val logs = ingestionCounters.map { counter =>
      val inputFiles = counter.paths.mkString(",")
      logger.info(
        s"ingestion-summary -> files: [$inputFiles], domain: ${domain.name}, schema: ${schema.name}, input: ${counter.inputCount}, accepted: ${counter.acceptedCount}, rejected:${counter.rejectedCount}"
      )
      val end = Timestamp.from(Instant.now())
      val success = !settings.appConfig.rejectAllOnError || counter.rejectedCount == 0
      buildAuditLog(
        inputFiles,
        success,
        counter.inputCount,
        counter.acceptedCount,
        counter.rejectedCount,
        start,
        end.getTime - start.getTime,
        if (success) "success" else s"${counter.rejectedCount} invalid records"
      )
    }
    AuditLog.sink(logs, accessToken)(settings, storageHandler, schemaHandler).map(_ => logs)
  }
}
