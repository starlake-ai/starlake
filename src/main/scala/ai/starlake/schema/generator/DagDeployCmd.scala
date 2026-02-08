package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

/** Command to deploy DAGs.
  *
  * Usage: starlake dag-deploy [options]
  */
object DagDeployCmd extends Cmd[DagDeployConfig] {
  val command = "dag-deploy"

  val parser: OParser[Unit, DagDeployConfig] = {
    val builder = OParser.builder[DagDeployConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("inputDir")
        .action((x, c) => c.copy(inputDir = Some(x)))
        .optional()
        .text(
          """Folder containing DAGs previously generated using the dag-generate command""".stripMargin
        ),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = x))
        .required()
        .text(
          """Path where to deploy the library files. This is the root of all DAGs""".stripMargin
        ),
      builder
        .opt[String]("dagDir")
        .action((x, c) => c.copy(dagDir = Some(x)))
        .optional()
        .text(
          """outputDir's sub-directory where to deploy the DAG files.""".stripMargin
        ),
      builder
        .opt[Unit]("clean")
        .action((_, c) => c.copy(clean = true))
        .optional()
        .text(
          """Should the output directory be deleted first ?""".stripMargin
        )
    )
  }

  def parse(args: Seq[String]): Option[DagDeployConfig] =
    OParser.parse(parser, args, DagDeployConfig(None, ""), setup)

  override def run(config: DagDeployConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    new DagDeployJob(schemaHandler).deployDags(config)
  }
}
