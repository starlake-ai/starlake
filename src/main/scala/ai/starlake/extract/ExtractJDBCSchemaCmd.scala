package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Success, Try}

trait ExtractJDBCSchemaCmd extends Cmd[ExtractSchemaConfig] {

  val command = "extract-schema"

  val parser: OParser[Unit, ExtractSchemaConfig] = {
    val builder = OParser.builder[ExtractSchemaConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      opt[String]("config")
        .action((x, c) => c.copy(extractConfig = x))
        .required()
        .text("Database tables & connection info"),
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Where to output YML files"),
      opt[Int]("parallelism")
        .action((x, c) => c.copy(parallelism = Some(x)))
        .optional()
        .text(
          s"parallelism level of the extraction process. By default equals to the available cores"
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class JDBC2YmlConfig.
    */
  def parse(args: Seq[String]): Option[ExtractSchemaConfig] =
    OParser.parse(parser, args, ExtractSchemaConfig(), setup)

  override def run(config: ExtractSchemaConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    new ExtractJDBCSchema(schemaHandler).run(config)
    Success(JobResult.empty)
  }
}

object ExtractJDBCSchemaCmd extends ExtractJDBCSchemaCmd
