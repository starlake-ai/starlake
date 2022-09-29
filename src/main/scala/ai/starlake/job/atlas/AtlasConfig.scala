package ai.starlake.job.atlas

import ai.starlake.utils.CliConfig
import scopt.OParser

case class AtlasConfig(
  delete: Boolean = false,
  folder: Option[String] = None,
  uris: Option[List[String]] = None,
  user: Option[String] = None,
  password: Option[String] = None,
  files: List[String] = Nil
)

object AtlasConfig extends CliConfig[AtlasConfig] {
  val command = "atlas"
  val parser: OParser[Unit, AtlasConfig] = {
    val builder = OParser.builder[AtlasConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[Unit]("delete")
        .action((_, c) => c.copy(delete = true))
        .optional()
        .text("Should we delete the previous schemas ?"),
      opt[String]("folder")
        .action((x, c) => c.copy(folder = Some(x)))
        .optional()
        .text("Folder with yaml schema files"),
      opt[String]("uris")
        .action((x, c) => c.copy(uris = Some(x.split(",").toList.map(_.trim))))
        .optional()
        .text("Atlas URI"),
      opt[String]("user")
        .action((x, c) => c.copy(user = Some(x)))
        .optional()
        .text("Atlas User"),
      opt[String]("password")
        .action((x, c) => c.copy(password = Some(x)))
        .optional()
        .text("Atlas password"),
      opt[String]("files")
        .action((x, c) => c.copy(files = x.split(",").toList.map(_.trim)))
        .optional()
        .text("List of YML files")
    )
  }
  // comet atlas  --delete --files file1,file2 --folder uri
  def parse(args: Seq[String]): Option[AtlasConfig] = OParser.parse(parser, args, AtlasConfig())
}
