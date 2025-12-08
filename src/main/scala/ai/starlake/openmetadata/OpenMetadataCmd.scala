package ai.starlake.openmetadata

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, JsonSerializer, SparkJobResult, Utils}
import scopt.OParser

import scala.util.{Failure, Success, Try}

trait OpenMetadataCmd extends Cmd[OpenMetadataConfig] {

  def command = "settings"

  val parser: OParser[Unit, OpenMetadataConfig] = {
    val builder = OParser.builder[OpenMetadataConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("option1")
        .action((x, c) => c.copy(oprion1 = Some(x)))
        .optional()
        // .required()
        .text("Enter option 1")
    )
  }

  def parse(args: Seq[String]): Option[OpenMetadataConfig] =
    OParser.parse(
      parser,
      args,
      OpenMetadataConfig()
    )

  override def run(config: OpenMetadataConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try {
      new OpenMetadataJob().run(config)

    } match {
      case Success(_) =>
        Success(JobResult.empty)
      case Failure(exception) =>
        Failure(exception)
    }
  }
}

object SettingsCmd extends OpenMetadataCmd
