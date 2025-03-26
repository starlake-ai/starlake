package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

trait Yml2XlsCmd extends Cmd[Yml2XlsConfig] {

  override def command: String = "yml2xls"

  val parser: OParser[Unit, Yml2XlsConfig] = {
    val builder = OParser.builder[Yml2XlsConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, "$command", "[options]"),
      builder.note(""),
      builder
        .opt[Seq[String]]("domain")
        .action((x, c) => c.copy(domains = x))
        .optional()
        .text("domains to convert to XLS"),
      builder
        .opt[String]("iamPolicyTagsFile")
        .action((x, c) => c.copy(iamPolicyTagsFile = Some(x)))
        .optional()
        .text(
          "IAM PolicyTag file to convert to XLS, SL_METADATA/iam-policy-tags.sl.yml by default)"
        ),
      builder
        .opt[String]("xls")
        .action((x, c) => c.copy(xlsDirectory = x))
        .required()
        .text("directory where XLS files are generated")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Yml2XlsConfig] =
    OParser.parse(parser, args, Yml2XlsConfig(), setup)

  override def run(config: Yml2XlsConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try(new Yml2Xls(schemaHandler).generateXls(config.domains, config.xlsDirectory)).map(_ =>
      JobResult.empty
    )
}

object Yml2XlsCmd extends Yml2XlsCmd
