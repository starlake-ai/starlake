package ai.starlake.schema.generator

import better.files.File
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model._
import ai.starlake.utils.CliConfig
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import ai.starlake.utils.YamlSerializer._
import scopt.OParser

object Xls2YmlAutoJob extends LazyLogging {

  def generateSchema(
    inputPath: String,
    policyPath: Option[String] = None,
    outputPath: Option[String] = None
  )(implicit
    settings: Settings
  ): Unit = {
    val reader = new XlsAutoJobReader(InputPath(inputPath), policyPath.map(InputPath))
    reader.autoJobsDesc
      .filter(_.tasks.exists(_.attributesDesc.nonEmpty))
      .foreach { autojob =>
        writeAutoJobYaml(
          autojob,
          outputPath.getOrElse(DatasetArea.jobs.toString),
          autojob.name
        )
      }
  }

  def writeAutoJobYaml(autoJobDesc: AutoJobDesc, outputPath: String, fileName: String)(implicit
    settings: Settings
  ): Unit = {
    logger.info(s"""Generated autoJob schemas:
                   |${serialize(autoJobDesc)}""".stripMargin)
    serializeToFile(File(outputPath, s"$fileName.comet.yml"), autoJobDesc)
  }

  def run(args: Array[String]): Boolean = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Xls2YmlAutoJobConfig.parse(args) match {
      case Some(config) =>
        config.files.foreach(generateSchema(_, config.policyFile, config.outputPath))
        true
      case _ =>
        println(Xls2YmlAutoJobConfig.usage())
        false
    }
  }

  def main(args: Array[String]): Unit = {
    val result = Xls2YmlAutoJob.run(args)
    System.exit(if (result) 0 else 1)
  }
}

case class Xls2YmlAutoJobConfig(
  files: Seq[String] = Nil,
  policyFile: Option[String] = None,
  outputPath: Option[String] = None
)

object Xls2YmlAutoJobConfig extends CliConfig[Xls2YmlAutoJobConfig] {
  val command = "xls2ymljob"

  val parser: OParser[Unit, Xls2YmlAutoJobConfig] = {
    val builder = OParser.builder[Xls2YmlAutoJobConfig]
    import builder._
    OParser.sequence(
      programName("starlake xls2ymljob"),
      head("starlake", "xls2ymljob", "[options]"),
      note(""),
      opt[Seq[String]]("files")
        .action((x, c) => c.copy(files = x))
        .required()
        .text("List of Excel files describing Domains & Schemas"),
      opt[Option[String]]("policyFile")
        .action((x, c) => c.copy(policyFile = x))
        .optional()
        .text(
          """File contains policies""".stripMargin
        ),
      opt[Option[String]]("outputPath")
        .action((x, c) => c.copy(outputPath = x))
        .optional()
        .text(
          """Path for saving the resulting YAML file(s). Comet domains path is used by default.""".stripMargin
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Xls2YmlAutoJobConfig] =
    OParser.parse(parser, args, Xls2YmlAutoJobConfig())

}
