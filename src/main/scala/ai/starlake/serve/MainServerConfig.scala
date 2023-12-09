package ai.starlake.serve

import ai.starlake.utils.CliConfig
import scopt.OParser

case class MainServerConfig(host: Option[String] = None, port: Option[Int] = None)

object MainServerConfig extends CliConfig[MainServerConfig] {
  val command = "serve"
  val parser: OParser[Unit, MainServerConfig] = {
    val builder = OParser.builder[MainServerConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
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

}
