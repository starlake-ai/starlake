package ai.starlake.schema.handlers

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.job.transform.AutoTask
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Failure, Success, Try}

object ValidateCmd extends Cmd[ValidateConfig] {

  val command = "validate"

  val parser: OParser[Unit, ValidateConfig] = {
    val builder = OParser.builder[ValidateConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text(
          "Reload all files from disk before starting validation. Always true regardless of the value set here."
        )
    )
  }

  override def parse(args: Seq[String]): Option[ValidateConfig] =
    OParser.parse(parser, args, ValidateConfig(), setup)

  override def run(config: ValidateConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    validateConnections()
    val errorsAndWarning = schemaHandler.checkValidity(config)
    errorsAndWarning match {
      case Failure(error) =>
        // scalastyle:off println
        println(error)
        error.printStackTrace()
      case Success((errorsAndWarning, errorCount, warningCount)) =>
        if (errorCount > 0) {
          // scalastyle:off println
          println(s"Found $errorCount errors")
        } else if (warningCount > 0) {
          // scalastyle:off println
          println(s"Found $warningCount warnings")
        } else
          // scalastyle:off println
          println("No errors or warnings found")
        errorsAndWarning.foreach { msg =>
          println(msg.toString())
        }
    }
    errorsAndWarning.map(_ => JobResult.empty)
  }

  def validateConnections()(implicit
    settings: Settings
  ) = {
    settings.appConfig.connections.keys.foreach { connectionName =>
      AutoTask
        .executeQuery(
          "__ignore__",
          "__ignore__",
          "SELECT 1",
          summarizeOnly = false,
          connectionName,
          None,
          test = false
        )(
          settings,
          settings.storageHandler(),
          settings.schemaHandler()
        ) match {
        case Success(_) =>
          // scalastyle:off println
          println(s"SUCCESS: Connection $connectionName is valid")
        case Failure(exception) =>
          // scalastyle:off println
          println(s"ERROR: Could not connect to database using connection $connectionName ")
      }
    }
  }
}
