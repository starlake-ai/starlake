package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import org.apache.hadoop.fs.Path
import scopt.OParser

import scala.util.Try

object IngestCmd extends Cmd[IngestConfig] {

  val command = "ingest"

  val parser: OParser[Unit, IngestConfig] = {
    val builder = OParser.builder[IngestConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      arg[String]("domain")
        .optional()
        .action((x, c) => c.copy(domain = x))
        .text("Domain name"),
      arg[String]("schema")
        .optional()
        .action((x, c) => c.copy(schema = x))
        .text("Schema name"),
      arg[String]("paths")
        .optional() // Some Ingestion Engine are not based on paths.$ eq. JdbcIngestionJob
        .action((x, c) => c.copy(paths = x.split(',').map(new Path(_)).toList))
        .text("list of comma separated paths"),
      arg[Map[String, String]]("options")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[IngestConfig] = {
    OParser.parse(parser, args, IngestConfig(), setup)
  }

  override def run(config: IngestConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).load(config).map(_ => JobResult.empty)
}
