package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

case class IamPoliciesConfig(accessToken: Option[String])

object IamPoliciesCmd extends Cmd[IamPoliciesConfig] {

  val command = "iam-policies"

  val parser: OParser[Unit, IamPoliciesConfig] = {
    val builder = OParser.builder[IamPoliciesConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[IamPoliciesConfig] =
    OParser.parse(parser, args, IamPoliciesConfig(accessToken = None), setup)

  override def run(config: IamPoliciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).applyIamPolicies(accessToken = None).map(_ => JobResult.empty)
}
