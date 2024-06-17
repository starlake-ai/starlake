package ai.starlake.job.infer

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Format, WriteMode}
import ai.starlake.utils.JobResult
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import scopt.OParser

import scala.util.{Failure, Success, Try}

object InferSchemaCmd extends Cmd[InferSchemaConfig] with StrictLogging {

  val command = "infer-schema"

  val parser: OParser[Unit, InferSchemaConfig] = {
    val builder = OParser.builder[InferSchemaConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domainName = x))
        .text("Domain Name"),
      builder
        .opt[String]("table")
        .action((x, c) => c.copy(schemaName = x))
        .text("Table Name"),
      builder
        .opt[String]("input")
        .action((x, c) => c.copy(inputPath = x))
        .required()
        .text("Dataset Input Path"),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Domain YAML Output Path"),
      builder
        .opt[String]("write")
        .action((x, c) => c.copy(write = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = Some(Format.fromString(x))))
        .optional()
        .text("Force input file format"),
      builder
        .opt[String]("rowTag")
        .action((x, c) => c.copy(rowTag = Some(x)))
        .optional()
        .text("row tag to use if detected format is XML"),
      builder
        .opt[Unit]("clean")
        .action((_, c) => c.copy(clean = true))
        .optional()
        .text("Delete previous YML before writing")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class InferSchemaConfig.
    */
  def parse(args: Seq[String]): Option[InferSchemaConfig] =
    OParser.parse(parser, args, InferSchemaConfig(), setup)

  override def run(config: InferSchemaConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val inputFile = File(config.inputPath)
    val inputPaths =
      if (inputFile.isDirectory()) {
        inputFile.list.map(_.pathAsString).toList.filter(!_.startsWith(".")) // filter hidden files
      } else {
        List(config.inputPath)
      }

    val results = inputPaths.map { inputPath =>
      logger.info(s"Inferring schema for $inputPath")
      workflow(schemaHandler)
        .inferSchema(
          config.copy(inputPath = inputPath)
        ) match {
        case Success(_) =>
          logger.info(s"Successfully inferred schema for $inputPath")
          Success(JobResult.empty)
        case Failure(exception) =>
          logger.error(s"Failed to infer schema for $inputPath", exception)
          Failure(exception)
      }
    }

    val failures = results.filter(_.isFailure).map {
      case Failure(exception) => exception.getMessage()
      case _                  => throw new IllegalStateException("This should never happen")
    }

    if (failures.isEmpty) {
      Success(JobResult.empty)
    } else {
      Failure(new Exception(failures.mkString("\n")))
    }
  }
}
