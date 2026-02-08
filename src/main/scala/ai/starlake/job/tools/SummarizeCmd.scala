package ai.starlake.job.tools

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, JsonSerializer, Utils}
import scopt.OParser

import scala.util.Try

/** Command to display table summary.
  *
  * Usage: starlake summarize [options]
  */
trait SummarizeCmd extends Cmd[SummarizeConfig] {

  def command = "summarize"

  val parser: OParser[Unit, SummarizeConfig] = {
    val builder = OParser.builder[SummarizeConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .valueName("domain1")
        .required()
        .text("Domain Name"),
      builder
        .opt[String]("table")
        .valueName("table")
        .required()
        .action((x, c) => c.copy(table = x))
        .text("Tables Name"),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[SummarizeConfig] =
    OParser.parse(
      parser,
      args,
      SummarizeConfig()
    )

  override def run(config: SummarizeConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val defaultConnection = settings.appConfig.connectionRef
    val result = AutoTask.executeSelect(
      config.domain,
      config.table,
      "",
      summarizeOnly = true,
      defaultConnection,
      config.accessToken,
      test = false,
      parseSQL = false,
      1000,
      1,
      None
    )(
      settings,
      settings.storageHandler(),
      schemaHandler
    )
    if (ai.starlake.job.Main.cliMode)
      println(">>>>>>")
    result.map { res =>
      Utils.printOut(JsonSerializer.serializeObject(res))
      JobResult.empty
    }
  }
}

object SummarizeCmd extends SummarizeCmd
