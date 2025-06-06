package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

trait TransformCmd extends Cmd[TransformConfig] {

  def command = "transform"

  val parser: OParser[Unit, TransformConfig] = {
    val builder = OParser.builder[TransformConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("name")
        .action((x, c) => c.copy(name = x))
        .required()
        .text("Task Name in the form domain.task"),
      builder
        .opt[Unit]("compile")
        .action((_, c) => c.copy(compile = true))
        .optional()
        .text("Return final query only"),
      builder
        .opt[String]("query")
        .action((x, c) => c.copy(query = Some(x)))
        .optional()
        .text("Run this query instead of the one in the task"),
      builder
        .opt[Unit]("dry-run")
        .action((_, c) => c.copy(dryRun = true))
        .optional()
        .text("Dry run only (supported on BigQuery only)"),
      builder
        .opt[Seq[String]]("tags")
        .action((x, c) => c.copy(tags = x))
        .optional()
        .text("Return final query only"),
      builder
        .opt[Unit]("format")
        .action((_, c) => c.copy(format = true))
        .optional()
        .text("pretty print the final query and update sql file"),
      builder
        .opt[String]("interactive")
        .action((x, c) => c.copy(interactive = Some(x)))
        .optional()
        .text("Run query without sinking the result"),
      builder
        .opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text("Reload YAML  files. Used in server mode"),
      builder
        .opt[Boolean]("truncate")
        .action((x, c) => c.copy(truncate = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      builder
        .opt[Int]("pageSize")
        .action((x, c) => c.copy(pageSize = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      builder
        .opt[Int]("pageNumber")
        .action((x, c) => c.copy(pageNumber = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      builder
        .opt[Unit]("recursive")
        .action((_, c) => c.copy(recursive = true))
        .optional()
        .text(
          s"Execute all dependencies recursively. Default value is false"
        ),
      builder
        .opt[Unit]("test")
        .action((_, c) => c.copy(test = true))
        .optional()
        .text(
          s"Should we run this transform as a test ? Default value is false"
        ),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = c.options ++ x))
        .unbounded()
        .text("Job arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[TransformConfig] = {
    val result = OParser.parse(parser, args, TransformConfig(), setup)
    result.foreach { config =>
      assert(config.name.isEmpty || config.tags.isEmpty, "Cannot specify both tags and task name")
    }
    result
  }

  override def run(config: TransformConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    if (config.compile) {
      workflow(schemaHandler).compileAutoJob(config).map(_ => JobResult.empty)
    } else {
      val res = workflow(schemaHandler).autoJob(config)
      res.map(_ => JobResult.empty)
    }
  }
}

object TransformCmd extends TransformCmd
