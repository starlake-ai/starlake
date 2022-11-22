package ai.starlake.job.bootstrap

import ai.starlake.utils.CliConfig
import scopt.OParser

case class BootstrapConfig(
  template: Option[String] = None
)

object BootstrapConfig extends CliConfig[BootstrapConfig] {
  val command = "bootstrap"
  val parser: OParser[Unit, BootstrapConfig] = {
    val builder = OParser.builder[BootstrapConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |Create a ne project optionally based on a specific template eq. quickstart / userguide
          |""".stripMargin
      ),
      opt[String]("template")
        .action((x, c) => c.copy(template = Some(x)))
        .text("Template to use to bootstrap project")
        .optional()
    )
  }

  // comet bqload  --source_file xxx --output_dataset domain --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  def parse(args: Seq[String]): Option[BootstrapConfig] =
    OParser.parse(parser, args, BootstrapConfig())

}
