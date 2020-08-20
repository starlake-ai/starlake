package com.ebiznext.comet.workflow

import com.ebiznext.comet.utils.CliConfig
import scopt.OParser

case class WatchConfig(includes: Seq[String] = Seq.empty, excludes: Seq[String] = Seq.empty)

object WatchConfig extends CliConfig[WatchConfig] {

  val parser: OParser[Unit, WatchConfig] = {
    val builder = OParser.builder[WatchConfig]
    import builder._
    OParser.sequence(
      programName("comet watch"),
      head("comet", "watch", "[options]"),
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
        .text("Domains not to watch")
    )
  }

  def parse(args: Seq[String]): Option[WatchConfig] =
    OParser.parse(parser, args, WatchConfig())
}
