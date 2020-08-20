package com.ebiznext.comet.utils

import org.fusesource.scalate.TemplateEngine
import scopt.{OParser, OptionDef}

trait CliConfig[T] {
  def parser: OParser[Unit, T]
  def usage(): String = OParser.usage(parser)
  def parse(args: Seq[String]): Option[T]
  val engine: TemplateEngine = new TemplateEngine

  def sphinx(): String = {
    val optionDefs = parser.toList
    val programNameOptionDef = optionDefs.headOption
    val synopsisOptionDef = programNameOptionDef.flatMap(_ => optionDefs.drop(1).headOption)
    val descriptionOptionDef = synopsisOptionDef.flatMap(_ => optionDefs.drop(2).headOption)
    val options = descriptionOptionDef.map(_ => optionDefs.drop(3)).getOrElse(Nil)

    val programName = programNameOptionDef.map(_.desc.substring("comet ".length)).getOrElse("")
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
      val rstExample = example.replace("example:", "\n.. code-block:: console\n\n  ")
      val rstDescription = rawText.replaceAll("\n", "\n| ") + "\n"
      rstDescription + rstExample
    }

    case class SphinxOption(
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
    def option(opt: OptionDef[_, T]): SphinxOption = {
      SphinxOption(
        opt.name,
        opt.valueName.getOrElse("<value>"),
        opt.desc.replaceAll("\n", "\n    "),
        if (opt.getMinOccurs > 0) "Required" else "Optional",
        if (opt.getMaxOccurs == Int.MaxValue) ", Unbounded" else ""
      )
    }

    val templateMap =
      Map(
        "programName" -> programName,
        "synopsis"    -> synopsis,
        "description" -> description,
        "options"     -> options.map(opt => option(opt).toMap())
      )

    //TODO keep the lines below until we depreciate Scala 2.11
    //     We'll replace it by --> val template = Source.fromResource("scalate/sphinx-cli.mustache").mkString

    val stream = getClass.getResourceAsStream("scalate/sphinx-cli.mustache")
    val template = scala.io.Source.fromInputStream(stream).mkString


    engine.layout(
      "sphinx-cli.mustache",
      engine.compileMoustache(template),
      templateMap
    )
  }
}
