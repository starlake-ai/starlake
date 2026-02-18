package ai.starlake.job.site

import better.files.File

import scala.util.Try

/** Site configuration Only one of templatePath and template should be defined
  * @param outputPath
  *   output path
  * @param templatePath
  *   template path
  * @param templateName
  *   template name
  */
import ai.starlake.job.ReportFormatConfig

case class SiteConfig(
  outputPath: File = File(".") / "site",
  templateName: Option[String] = Some("docusaurus"),
  format: Option[String] = Some("markdown"),
  clean: Option[Boolean] = Some(false)
) extends ReportFormatConfig {
  def reportFormat: Option[String] = format

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
