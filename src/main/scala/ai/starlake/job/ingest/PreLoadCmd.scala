package ai.starlake.job.ingest

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{EmptyJobResult, FailedJobResult, JobResult, PreLoadJobResult}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import scopt.OParser

import scala.util.{Success, Try}

trait PreLoadCmd extends Cmd[PreLoadConfig] with StrictLogging {

  def command = "preload"

  val parser: OParser[Unit, PreLoadConfig] = {
    val builder = OParser.builder[PreLoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .valueName("domain1")
        .required()
        .text("Domain to pre load"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to pre load"),
      builder
        .opt[String]("strategy")
        .optional()
        .action((x, c) => c.copy(strategy = PreLoadStrategy.fromString(x)))
        .text("pre load strategy"),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Pre load arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[PreLoadConfig] =
    OParser.parse(parser, args, PreLoadConfig(domain = "", accessToken = None))

  override def run(config: PreLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    logger.info(
      s"Pre loading domain ${config.domain} with strategy ${config.strategy.map(_.value).getOrElse("none")}"
    )

    config.strategy match {
      case Some(PreLoadStrategy.Imported) =>
        schemaHandler.domains(List(config.domain), config.tables.toList).headOption match {
          case Some(domain) =>
            Success(
              PreLoadJobResult(
                config.domain,
                workflow(schemaHandler)
                  .listStageFiles(domain, dryMode = true)
                  .map(kv => kv._1 -> kv._2.size)
              )
            )
          case None =>
            Success(PreLoadJobResult(config.domain, config.tables.map(t => t -> 0).toMap))
        }

      case Some(PreLoadStrategy.Pending) =>
        val pendingArea = DatasetArea.stage(config.domain)
        val files = settings.storageHandler().list(pendingArea, recursive = false)
        val results =
          for (table <- config.tables) yield {
            schemaHandler.getSchema(config.domain, table) match {
              case Some(schema) =>
                table -> files.count(file => schema.pattern.matcher(file.path.getName).matches())
              case None =>
                table -> 0
            }
          }

        Success(PreLoadJobResult(config.domain, results.toMap))

      case Some(PreLoadStrategy.Ack) =>
        config.globalAckFilePath match {
          case Some(globalAckFilePath) =>
            val storageHandler = settings.storageHandler()
            val path = new Path(globalAckFilePath)
            if (storageHandler.exists(path)) {
              storageHandler.delete(path)
              Success(EmptyJobResult)
            } else {
              Success(FailedJobResult)
            }
          case None =>
            Success(FailedJobResult)
        }

      case _ =>
        Success(PreLoadJobResult(config.domain, config.tables.map(t => t -> 1).toMap))
    }
  }
}

object PreLoadCmd extends PreLoadCmd
