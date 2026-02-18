package ai.starlake.migration

import ai.starlake.job.ReportFormatConfig
import ai.starlake.utils.CliConfig
import scopt.OParser

case class MigrateConfig(
  template: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig

object MigrateConfig extends CliConfig[MigrateConfig] {
  val command = "migrate"
  val parser: OParser[Unit, MigrateConfig] = {
    val builder = OParser.builder[MigrateConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |Migrate current project to the latest version available.
          |Print warning for any changes that requires user's attention.
          |Once project is migrated, check the difference and makes sure that everything is working as expected.
          |""".stripMargin
      ),
      opt[String]("reportFormat")
        .action((x, c) => c.copy(reportFormat = Some(x)))
        .text("Report format: console, json, html")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[MigrateConfig] =
    OParser.parse(parser, args, MigrateConfig(), setup)

}
