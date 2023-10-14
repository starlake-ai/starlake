package ai.starlake.job.ingest

import ai.starlake.utils.CliConfig
import scopt.OParser

case class LoadConfig(
  domains: Seq[String] = Seq.empty,
  tables: Seq[String] = Seq.empty,
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
      opt[Seq[String]]("domains")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to watch"),
      opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to watch"),
      opt[Seq[String]]("include")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Deprecated: Domains to watch"),
      opt[Seq[String]]("schemas")
        .valueName("schema1,schema2,schema3...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Deprecated: Schemas to watch"),
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
