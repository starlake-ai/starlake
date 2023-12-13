package ai.starlake.utils

import scopt.{DefaultOEffectSetup, DefaultOParserSetup, OEffectSetup, OParser, OParserSetup}

case class CliEnvConfig(options: Map[String, String] = Map.empty)

object CliEnvConfig extends CliConfig[CliEnvConfig] {
  val command = "bootstrap"
  val parser: OParser[Unit, CliEnvConfig] = {
    val builder = OParser.builder[CliEnvConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |Create a new project optionally based on a specific template eq. quickstart / userguide
          |""".stripMargin
      ),
      opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = x))
        .text("BigQuery Sink Options")
        .optional()
    )
  }
  private[this] lazy val psetup = new DefaultOParserSetup with OParserSetup {
    override def errorOnUnknownArgument: Boolean = false
  }
  private[this] lazy val esetup = new DefaultOEffectSetup with OEffectSetup {
    override def displayToOut(msg: String): Unit = ()
    override def displayToErr(msg: String): Unit = ()
    override def reportError(msg: String): Unit = ()
    override def reportWarning(msg: String): Unit = ()
    override def terminate(exitState: Either[String, Unit]): Unit = ()
  }

  def parse(args: Seq[String]): Option[CliEnvConfig] =
    OParser.parse(parser, args, CliEnvConfig(), psetup, esetup)

}
