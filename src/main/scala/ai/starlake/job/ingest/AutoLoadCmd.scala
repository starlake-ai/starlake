package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.job.infer.{InferSchemaCmd, InferSchemaConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, SparkJobResult, Utils}
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import scopt.OParser

import scala.util.{Failure, Success, Try}

trait AutoLoadCmd extends Cmd[AutoLoadConfig] with StrictLogging {

  def command = "autoload"

  val parser: OParser[Unit, AutoLoadConfig] = {
    val builder = OParser.builder[AutoLoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[Seq[String]]("domains")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to watch"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to watch"),
      builder
        .opt[Unit]("clean")
        .optional()
        .action((x, c) => c.copy(clean = true))
        .text("Overwrite existing mapping files before starting"),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Watch arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[AutoLoadConfig] =
    OParser.parse(parser, args, AutoLoadConfig())

  override def run(config: AutoLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val incomingFile = File(settings.appConfig.incoming)
    if (incomingFile.exists && incomingFile.isDirectory) {
      val folders =
        incomingFile.list.filter(f =>
          f.isDirectory && !f.name.startsWith(".") && !f.name.startsWith("_")
        )
      val success = folders
        .map { folder =>
          val domainName = folder.name
          if (config.domains.nonEmpty && !config.domains.contains(domainName)) {
            logger.info(s"Skipping $domainName")
            false
          } else {
            logger.info(s"Processing ${folder.pathAsString}")
            InferSchemaCmd
              .run(
                InferSchemaConfig(
                  inputPath = folder.pathAsString,
                  clean = config.clean
                ),
                schemaHandler
              )
              .isSuccess
          }
        }
        .forall(identity)
      if (success) {
        val wf = workflow(schemaHandler)
        logger.info("All schemas inferred successfully")
        wf.stage(StageConfig(config.domains)).map { jb =>
          logger.info("Staged successfully")
          wf.load(LoadConfig(config.domains, config.tables, config.options)) match {
            case Success(true) =>
              logger.info("Loaded successfully")
              SparkJobResult(None)
            case Success(false) =>
              throw new Exception("Failed to load")
            case Failure(exception) =>
              Utils.logException(logger, exception)
              throw exception
          }
        }
      } else {
        throw new Exception("Some schemas failed to be inferred")
      }
    } else {
      throw new Exception(s"${settings.appConfig.incoming} is not a directory")
    }
  }
}

object AutoLoadCmd extends AutoLoadCmd
