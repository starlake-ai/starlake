package ai.starlake.workflow

import ai.starlake.utils.CliConfig
import scopt.OParser

case class ImportConfig(
  includes: Seq[String] = Seq.empty
)

object ImportConfig extends CliConfig[ImportConfig] {
  val command = "import"

  val parser: OParser[Unit, ImportConfig] = {
    val builder = OParser.builder[ImportConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note("""
          |Move the files from the landing area to the pending area.
          |
          |Files are loaded one domain at a time.
          |
          |Each domain has its own directory and is specified in the "directory" key of Domain YML file
          |compressed files are uncompressed if a corresponding ack file exist.
          |
          |Compressed files are recognized by their extension which should be one of .tgz, .zip, .gz.
          |raw file should also have a corresponding ack file
          |before moving the files to the pending area, the ack files are deleted.
          |
          |To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML file.
          |
          |"ack" is the default ack extension searched for but you may specify a different one in the domain YML file.
          |example: comet import
          |""".stripMargin),
      opt[Seq[String]]("include")
        .action((x, c) => c.copy(includes = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to import")
    )
  }

  def parse(args: Seq[String]): Option[ImportConfig] = OParser.parse(parser, args, ImportConfig())
}
