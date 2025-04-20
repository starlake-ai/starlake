package ai.starlake.utils

import ai.starlake.config.Settings
import better.files.File
import scopt.{DefaultOParserSetup, OParser, OParserSetup, OptionDef}

import scala.io.Source
import scala.jdk.CollectionConverters._

trait CommandConfig {
  def command: String
  def markdown(pageIndex: Int)(implicit settings: Settings): String
}

trait CliConfig[T] extends CommandConfig {
  def parser: OParser[Unit, T]
  def usage(): String = OParser.usage(parser)
  def parse(args: Seq[String]): Option[T]
  def command: String

  val setup: OParserSetup = new DefaultOParserSetup {
    override def showUsageOnError: Option[Boolean] = Some(false)
  }

  def markdown(pageIndex: Int)(implicit settings: Settings): String = {
    val optionDefs = parser.toList
    val programNameOptionDef = optionDefs.headOption
    val synopsisOptionDef = programNameOptionDef.flatMap(_ => optionDefs.drop(1).headOption)
    val descriptionOptionDef = synopsisOptionDef.flatMap(_ => optionDefs.drop(2).headOption)
    val options = descriptionOptionDef.map(_ => optionDefs.drop(3)).getOrElse(Nil)

    val programName = programNameOptionDef.map(_.desc.substring("starlake ".length)).getOrElse("")
    val synopsis = synopsisOptionDef.map(_.desc).getOrElse("")
    val rawDescription = descriptionOptionDef
      .map(_.desc)
      .getOrElse("")

    val description = {
      val indexExample = rawDescription.indexOf("example:")
      val (rawText, example) =
        if (indexExample >= 0) {
          (rawDescription.substring(0, indexExample), rawDescription.substring(indexExample))
        } else {
          (rawDescription, "")
        }
      val mdExample = example.replace("example:", "\n\n")
      val mdDescription = rawText
      if (mdExample.trim.nonEmpty)
        mdDescription + "````shell\n" + mdExample.trim + "\n````\n"
      else
        mdDescription
    }

    val mdExtraDescriptionFile =
      File(getClass.getResource("/")) / s"../../../docs/merge/cli/$programName.md"
    val extra =
      if (mdExtraDescriptionFile.exists())
        mdExtraDescriptionFile.lines().mkString("\n")
      else
        ""

    case class MarkdownOption(
      name: String,
      value: String,
      description: String,
      required: String,
      unbounded: String
    ) {
      def toMap(): Map[String, String] =
        Map(
          "name"        -> name,
          "value"       -> value,
          "description" -> description,
          "required"    -> required,
          "unbounded"   -> unbounded
        )
    }
    def option(opt: OptionDef[_, T]): MarkdownOption = {
      MarkdownOption(
        opt.name,
        opt.valueName.getOrElse("<value>"),
        opt.desc.replaceAll("\n", "<br />"),
        if (opt.getMinOccurs > 0) "Required" else "Optional",
        if (opt.getMaxOccurs == Int.MaxValue) ", Unbounded" else ""
      )
    }

    val templateMap =
      Map(
        "programName" -> programName,
        "synopsis"    -> synopsis,
        "description" -> description,
        "options"     -> options.map(opt => option(opt).toMap().asJava).toArray,
        "index"       -> (pageIndex * 10).toString,
        "extra"       -> extra
      )

    // TODO keep the lines below until we depreciate Scala 2.11
    //     We'll replace it by --> val template = Source.fromResource("templates/cli/md-cli.mustache").mkString

    val template = Source.fromResource("templates/cli/md-cli.j2").mkString
    Utils.parseJinja(template, templateMap)
  }
}
