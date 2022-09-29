package ai.starlake.workflow

import ai.starlake.utils.CliConfig
import scopt.OParser

case class WatchConfig(
  includes: Seq[String] = Seq.empty,
  excludes: Seq[String] = Seq.empty,
  schemas: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty
)

object WatchConfig extends CliConfig[WatchConfig] {
  val command = "watch"

  val parser: OParser[Unit, WatchConfig] = {
    val builder = OParser.builder[WatchConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[Seq[String]]("include")
        .action((x, c) => c.copy(includes = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to watch"),
      opt[Seq[String]]("exclude")
        .valueName("domain1,domain2...")
        .optional()
        .action((x, c) => c.copy(excludes = x))
        .text("Domains not to watch"),
      opt[Seq[String]]("schemas")
        .valueName("schema1,schema2,schema3...")
        .optional()
        .action((x, c) => c.copy(schemas = x))
        .text("Schemas to watch"),
      opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Watch arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[WatchConfig] =
    OParser.parse(parser, args, WatchConfig())
}
