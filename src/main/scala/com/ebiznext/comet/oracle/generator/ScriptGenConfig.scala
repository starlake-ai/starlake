package com.ebiznext.comet.oracle.generator

import better.files.File
import scopt.{OParser, RenderingMode}

case class ScriptGenConfig(
  referentialFile: File = File("."),
  scriptTemplateFile: File = File("."),
  scriptOutputDir: File = File(".")
)

object ScriptGenConfig {

  val builder = OParser.builder[ScriptGenConfig]

  def exists(name: String)(path: String): Either[String, Unit] =
    if (File(path).exists) Right(())
    else Left(s"$name at path $path does not exist")

  val parser: OParser[Unit, ScriptGenConfig] = {
    import builder._
    OParser.sequence(
      programName("comet"),
      cmd("script-gen"),
      head("comet", "1.x"),
      opt[String]("referentialFile")
        .validate(exists("Excel referential file"))
        .action((x, c) => c.copy(referentialFile = File(x)))
        .required()
        .text("Excel referential file"),
      opt[String]("templateFile")
        .validate(exists("Script template file"))
        .action((x, c) => c.copy(scriptTemplateFile = File(x)))
        .required()
        .text("Script template file"),
      opt[String]("scriptsOutputDir")
        .validate(exists("Script output folder"))
        .action((x, c) => c.copy(scriptOutputDir = File(x)))
        .required()
        .text("Scripts output folder")
    )
  }
  val usage: String = OParser.usage(parser, RenderingMode.TwoColumns)

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args : Command line parameters
    * @return : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[ScriptGenConfig] =
    OParser.parse(parser, args, ScriptGenConfig.apply())
}
