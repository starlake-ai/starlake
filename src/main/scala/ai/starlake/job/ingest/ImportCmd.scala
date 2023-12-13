package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object ImportCmd extends Cmd[ImportConfig] {

  val command = "import"

  val parser: OParser[Unit, ImportConfig] = {
    val builder = OParser.builder[ImportConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note("""
             |Move the files from the landing area to the pending area.
             |
             |Files are loaded one domain at a time.
             |
             |Each domain has its own directory and is specified in the "directory" key of Domain YML file
             |compressed files are uncompressed if a corresponding ack file exist.
             |
             |Compressed files are recognized by their extension which should be one of .tgz, .zip, .gz.
             |raw file should also have a corresponding ack file
             |before moving the files to the pending area, the ack files are deleted.
             |
             |To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML file.
             |
             |"ack" is the default ack extension searched for but you may specify a different one in the domain YML file.
             |example: comet import
             |""".stripMargin),
      builder
        .opt[Seq[String]]("include")
        .action((x, c) => c.copy(includes = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to import")
    )
  }

  def parse(args: Seq[String]): Option[ImportConfig] =
    OParser.parse(parser, args, ImportConfig(), setup)

  override def run(config: ImportConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).loadLanding(config).map(_ => JobResult.empty)
}
