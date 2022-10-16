package ai.starlake.job.serve

import ai.starlake.utils.CliConfig
import scopt.OParser

case class MainServerConfig(port: Int = 7070)
object MainServerConfig extends CliConfig[MainServerConfig] {
  val command = "serve"
  val parser: OParser[Unit, MainServerConfig] = {
    val builder = OParser.builder[MainServerConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[Int]("port")
        .action((x, c) => c.copy(port = x))
        .optional()
        .text("Port on which the server is listening")
    )
  }

  override def parse(args: Seq[String]): Option[MainServerConfig] =
    OParser.parse(parser, args, MainServerConfig())

}
