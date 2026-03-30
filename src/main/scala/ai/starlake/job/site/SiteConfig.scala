package ai.starlake.job.site

import better.files.File

import ai.starlake.job.ReportFormatConfig

/** Site configuration
  * @param outputPath
  *   output path
  * @param templateName
  *   template name or path to custom templates
  */
case class SiteConfig(
  outputPath: File = File(".") / "site",
  templateName: Option[String] = Some("standalone"),
  format: Option[String] = Some("markdown"),
  clean: Option[Boolean] = Some(false)
) extends ReportFormatConfig {
  def reportFormat: Option[String] = format
}
