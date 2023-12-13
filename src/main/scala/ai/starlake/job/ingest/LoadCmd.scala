package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

trait LoadCmd extends Cmd[LoadConfig] {

  val command = "load"

  val parser: OParser[Unit, LoadConfig] = {
    val builder = OParser.builder[LoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[Seq[String]]("domains")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to watch"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to watch"),
      builder
        .opt[Seq[String]]("include")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Deprecated: Domains to watch"),
      builder
        .opt[Seq[String]]("schemas")
        .valueName("schema1,schema2,schema3...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Deprecated: Schemas to watch"),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Watch arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[LoadConfig] =
    OParser.parse(parser, args, LoadConfig())

  override def run(config: LoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).loadPending(config).map(_ => JobResult.empty)
}

object LoadCmd extends LoadCmd
