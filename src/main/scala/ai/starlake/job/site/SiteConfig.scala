package ai.starlake.job.site

import ai.starlake.utils.CliConfig
import better.files.File
import scopt.OParser

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
      opt[String]("output-path")
        .action((x, c) => c.copy(outputPath = File(x)))
        .text("Output path")
        .optional(),
      opt[String]("template-name")
        .action((x, c) => c.copy(templateName = Some(x)))
        .text("Template to use. See templates/site/ folder for available templates")
        .optional(),
      opt[String]("template-path")
        .action((x, c) => c.copy(templatePath = Some(File(x))))
        .text("Custom template path to use. See templates/site/ folder for folder structure")
        .optional()
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
  templatePath: Option[File] = None,
  templateName: Option[String] = None
) {

  private def templateContentFromResource(templateType: String): (String, String) = {
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

  private def templateContentFromFile(templateType: String): (String, String) = {
    val sspFile = templatePath.map(File(_, s"/$templateType.ssp")).getOrElse {
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
    // make sure only template path or template name is defined
    if (templatePath.isDefined && templateName.isDefined) {
      throw new IllegalArgumentException(
        "Only one of templatePath and template name should be defined"
      )
    }

    (templatePath, templateName) match {
      case (Some(_), None) => templateContentFromFile(templateType)
      case (None, Some(_)) => templateContentFromResource(templateType)
      case _ =>
        throw new IllegalArgumentException(
          "Either templatePath or template name should be defined"
        )
    }
  }
}
