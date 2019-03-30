package com.ebiznext.comet.job.infer
import scopt.OParser

case class InferConfig(
  domainName: String = "",
  inputPath: String = "",
  outputPath: String = "",
  header: Option[Boolean] = Some(false)
)

object InferSchemaConfig {

  def parse(args: Seq[String]): Option[InferConfig] = {
    val builder = OParser.builder[InferConfig]

    val parser: OParser[Unit, InferConfig] = {
      import builder._
      OParser.sequence(
        programName("comet"),
        head("comet", "1.x"),
        opt[String]("domain")
          .action((x, c) => c.copy(domainName = x))
          .required()
          .text("Domain Name"),
        opt[String]("input")
          .action((x, c) => c.copy(inputPath = x))
          .required()
          .text("Input Path"),
        opt[String]("output")
          .action((x, c) => c.copy(outputPath = x))
          .required()
          .text("Output Path"),
        opt[Option[Boolean]]("with-header")
          .action((x, c) => c.copy(header = x))
          .optional()
          .text("Does the file contain a header")
      )
    }
    OParser.parse(parser, args, InferConfig())

  }
}
