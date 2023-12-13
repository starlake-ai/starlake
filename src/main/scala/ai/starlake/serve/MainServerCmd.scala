package ai.starlake.serve

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object MainServerCmd extends Cmd[MainServerConfig] {
  override def command: String = "serve"

  val parser: OParser[Unit, MainServerConfig] = {
    val builder = OParser.builder[MainServerConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      opt[String]("host")
        .action((x, c) => c.copy(host = Some(x)))
        .optional()
        .text("address on which the server is listening"),
      opt[Int]("port")
        .action((x, c) => c.copy(port = Some(x)))
        .optional()
        .text("Port on which the server is listening")
    )
  }

  override def parse(args: Seq[String]): Option[MainServerConfig] =
    OParser.parse(parser, args, MainServerConfig(), setup)

  override def run(config: MainServerConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val host = config.host.getOrElse(settings.appConfig.http.interface)
    val port = config.port.getOrElse(settings.appConfig.http.port)
    SingleUserMainServer.serve(host, port).map(_ => JobResult.empty)
  }
}
