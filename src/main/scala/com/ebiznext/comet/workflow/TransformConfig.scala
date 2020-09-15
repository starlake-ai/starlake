package com.ebiznext.comet.workflow

import com.ebiznext.comet.utils.CliConfig
import scopt.OParser

case class TransformConfig(
  name: String = "",
  options: Map[String, String] = Map.empty,
  views: Seq[String] = Nil,
  viewsDir: Option[String] = None,
  viewsCount: Int = 1000
)

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
      opt[Seq[String]]("views")
        .action((x, c) => c.copy(views = x))
        .optional()
        .text("""view1,view2 ...
            |If present only the request present in these views statements are run. Useful for unit testing
            |""".stripMargin),
      opt[String]("views-dir")
        .action((x, c) => c.copy(viewsDir = Some(x)))
        .optional()
        .text("Where to store the result of the query in JSON"),
      opt[Int]("views-count")
        .action((x, c) => c.copy(viewsCount = x))
        .optional()
        .text(
          s"Max number of rows to retrieve. Negative value means the maximum value ${Int.MaxValue}"
        ),
      opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = x))
        .text("Job arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[TransformConfig] =
    OParser.parse(parser, args, TransformConfig())
}
