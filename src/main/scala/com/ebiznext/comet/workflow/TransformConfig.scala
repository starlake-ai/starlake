package com.ebiznext.comet.workflow

import com.ebiznext.comet.utils.CliConfig
import scopt.OParser

case class TransformConfig(name: String = "", options: Map[String, String] = Map.empty)

object TransformConfig extends CliConfig[TransformConfig] {
  val parser: OParser[Unit, TransformConfig] = {
    val builder = OParser.builder[TransformConfig]
    import builder._
    OParser.sequence(
      programName("comet transform | job"),
      head("comet", "transform | job", "[options]"),
      note(""),
      opt[String]("name")
        .action((x, c) => c.copy(name = x))
        .required()
        .text("Job Name"),
      opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = x))
        .text("Job arguments to be used as substitutions")
    )
  }


  def parse(args: Seq[String]): Option[TransformConfig] =
    OParser.parse(parser, args, TransformConfig())
}
