package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

object SiteCmd extends Cmd[SiteConfig] {

  val command = "site"

  val parser: OParser[Unit, SiteConfig] = {
    val builder = OParser.builder[SiteConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(
        """
          |Generate site
          |""".stripMargin
      ),
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputPath = File(x)))
        .text("Output Directory")
        .optional(),
      opt[String]("template")
        .action((x, c) => c.copy(templateName = Some(x)))
        .text("Template name or template path to use")
        .required()
    )
  }

  def parse(args: Seq[String]): Option[SiteConfig] =
    OParser.parse(parser, args, SiteConfig(), setup)

  val TABLE_TEMPLATE = "table"
  val TASK_TEMPLATE = "task"

  override def run(config: SiteConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    new SiteHandler(config, schemaHandler).run().map(_ => JobResult.empty)
}
