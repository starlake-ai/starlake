package ai.starlake.job.ingest

import ai.starlake.utils.CliConfig
import scopt.OParser

case class LoadConfig(
  includes: Seq[String] = Seq.empty,
  schemas: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty
)

object LoadConfig extends CliConfig[LoadConfig] {
  val command = "watch"

  val parser: OParser[Unit, LoadConfig] = {
    val builder = OParser.builder[LoadConfig]
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

  def parse(args: Seq[String]): Option[LoadConfig] =
    OParser.parse(parser, args, LoadConfig())
}
