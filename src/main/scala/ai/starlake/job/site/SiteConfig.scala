package ai.starlake.job.site

import ai.starlake.utils.CliConfig
import better.files.File
import scopt.OParser

import scala.util.Try

object SiteConfig extends CliConfig[SiteConfig] {
  val command = "site"
  val parser: OParser[Unit, SiteConfig] = {
    val builder = OParser.builder[SiteConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |Generate site
          |""".stripMargin
      ),
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputPath = File(x)))
        .text("Output Directory")
        .optional(),
      opt[String]("template")
        .action((x, c) => c.copy(templateName = Some(x)))
        .text("Template name or template path to use")
        .required()
    )
  }

  def parse(args: Seq[String]): Option[SiteConfig] =
    OParser.parse(parser, args, SiteConfig())

  val TABLE_TEMPLATE = "table"
  val TASK_TEMPLATE = "task"
}

/** Site configuration Only one of templatePath and template should be defined
  * @param outputPath
  *   output path
  * @param templatePath
  *   template path
  * @param templateName
  *   template name
  */
case class SiteConfig(
  outputPath: File = File(".") / "site",
  templateName: Option[String] = None
) {

  private def templateContentFromResource(templateType: String): Try[(String, String)] = Try {
    templateName match {
      case Some(name) =>
        val sspResource = s"/templates/site/$name/$templateType.ssp"
        val stream = getClass.getResourceAsStream(sspResource)
        (sspResource, scala.io.Source.fromInputStream(stream).mkString)
      case None =>
        throw new IllegalArgumentException(
          "Either templatePath or template name should be defined"
        )
    }
  }

  private def templateContentFromFile(templateType: String): Try[(String, String)] = Try {
    val sspFile = templateName.map(File(_, s"/$templateType.ssp")).getOrElse {
      throw new IllegalArgumentException(
        s"Template path is not defined, but template is not found: $templateType"
      )
    }
    (sspFile.pathAsString, sspFile.lines.mkString("\n"))
  }

  /** Returns template content by template type
    * @param templateType
    *   template type: see SiteConfig.*_TEMPLATE definitions
    * @return
    *   (template path, template content)
    */
  def templateContent(templateType: String): (String, String) = {
    templateContentFromResource(templateType).getOrElse(
      templateContentFromFile(templateType).getOrElse(
        throw new IllegalArgumentException(s"Template is not found: $templateType")
      )
    )
  }
}
