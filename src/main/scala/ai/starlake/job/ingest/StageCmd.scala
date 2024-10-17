package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object StageCmd extends Cmd[StageConfig] {

  val command = "stage"

  val parser: OParser[Unit, StageConfig] = {
    val builder = OParser.builder[StageConfig]
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
        .opt[Seq[String]]("domains")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to stage"),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = c.options ++ x))
        .unbounded()
        .text("Stage arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[StageConfig] =
    OParser.parse(parser, args, StageConfig(), setup)

  override def run(config: StageConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).stage(config).map(_ => JobResult.empty)
}

object ImportCmd extends Cmd[StageConfig] {

  override def run(config: StageConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    StageCmd.run(config, schemaHandler)

  override def parser: OParser[Unit, StageConfig] = StageCmd.parser

  override def parse(args: Seq[String]): Option[StageConfig] = StageCmd.parse(args)

  override def command: String = "import"
}
