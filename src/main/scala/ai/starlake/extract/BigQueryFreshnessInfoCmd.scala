package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.extract.BigQueryFreshnessInfo.freshness
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, JsonSerializer}

import scala.util.{Failure, Success, Try}

object BigQueryFreshnessInfoCmd extends BigQueryTablesCmd {
  override def command: String = "bq-freshness"

  override def run(config: BigQueryTablesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val result = Try(freshness(config, schemaHandler))
    result match {
      case Success(statuses) =>
        val warnFound = statuses.find(_.warnOrError == "WARN")
        val errFound = statuses.find(_.warnOrError == "ERROR")
        // scalastyle:off println
        println(JsonSerializer.serializeObject(result))
        if (errFound.isDefined)
          System.exit(2)
        if (warnFound.isDefined)
          System.exit(1)
      case Failure(_) =>
    }
    result.map(_ => JobResult.empty)
  }
}
