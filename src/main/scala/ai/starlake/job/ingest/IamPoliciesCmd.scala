package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

case class IamPoliciesConfig()

object IamPoliciesCmd extends Cmd[IamPoliciesConfig] {

  val command = "iam-policies"

  val parser: OParser[Unit, IamPoliciesConfig] = {
    val builder = OParser.builder[IamPoliciesConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command),
      builder.note("")
    )
  }

  def parse(args: Seq[String]): Option[IamPoliciesConfig] =
    OParser.parse(parser, args, IamPoliciesConfig(), setup)

  override def run(config: IamPoliciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).applyIamPolicies().map(_ => JobResult.empty)
}
