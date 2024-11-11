package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{EmptyJobResult, JobResult}
import com.typesafe.scalalogging.StrictLogging
import scopt.OParser

import scala.util.{Success, Try}

trait PreLoadCmd extends Cmd[PreLoadConfig] with StrictLogging {

  def command = "preload"

  val parser: OParser[Unit, PreLoadConfig] = {
    val builder = OParser.builder[PreLoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .valueName("domain1")
        .required()
        .text("Domain to pre load"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to pre load"),
      builder
        .opt[String]("strategy")
        .optional()
        .action((x, c) => c.copy(strategy = PreLoadStrategy.fromString(x)))
        .text("pre load strategy"),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Pre load arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[PreLoadConfig] =
    OParser.parse(parser, args, PreLoadConfig(domain = "*", accessToken = None))

  override def run(config: PreLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Success(EmptyJobResult)
  }
}

object PreLoadCmd extends PreLoadCmd
