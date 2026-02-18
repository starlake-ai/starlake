package ai.starlake.job.bootstrap

import ai.starlake.job.ReportFormatConfig
import ai.starlake.utils.CliConfig
import scopt.OParser

case class BootstrapConfig(
  template: Option[String] = None,
  noExit: Option[Boolean] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig

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
        .optional(),
      opt[String]("reportFormat")
        .action((x, c) => c.copy(reportFormat = Some(x)))
        .text("Report format: console, json, html")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[BootstrapConfig] =
    OParser.parse(parser, args, BootstrapConfig(), setup)

}
