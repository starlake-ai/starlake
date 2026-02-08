package ai.starlake.extract.freshness

import ai.starlake.config.Settings
import ai.starlake.extract.{TablesExtractCmd, TablesExtractConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, JsonSerializer}

import scala.util.{Failure, Success, Try}

/** Command to check for data freshness.
  *
  * Usage: starlake freshness [options]
  */
object FreshnessExtractCmd extends TablesExtractCmd {
  override def command: String = "freshness"

  override def run(config: TablesExtractConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val result = Try(FreshnessJob.freshness(config, schemaHandler))
    result match {
      case Success(statuses) =>
        val warnFound = statuses.find(_.warnOrError == "WARN")
        val errFound = statuses.find(_.warnOrError == "ERROR")
        // scalastyle:off println
        println(JsonSerializer.serializeObject(result))
        if (errFound.isDefined)
          System.exit(1)
      case Failure(_) =>
    }
    result.map(_ => JobResult.empty)
  }
}
