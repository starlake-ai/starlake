package ai.starlake.schema.generator

import ai.starlake.config.Settings
import better.files.Resource
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

abstract class AnyTemplateLoader extends LazyLogging {

  protected val TEMPLATE_FOLDER = "templates"
  protected def RESOURCE_TEMPLATE_FOLDER: String
  protected def EXTERNAL_TEMPLATE_BASE_PATH(implicit settings: Settings): Path

  def allPaths(implicit settings: Settings): List[String] = {
    List(
      EXTERNAL_TEMPLATE_BASE_PATH.toString,
      RESOURCE_TEMPLATE_FOLDER
    )
  }
  def loadTemplate(templatePathname: String)(implicit settings: Settings): String = {
    loadTemplateFromAbsolutePath(templatePathname)
      .orElse(loadTemplateFromAppPath(templatePathname))
      .orElse(loadTemplateFromResources(templatePathname))
      .getOrElse(
        throw new RuntimeException(
          s"Template `${templatePathname}` not found. Please provide an absolute path or a template returned by `list` command."
        )
      )
  }

  def loadTemplateFromAbsolutePath(
    template: String
  )(implicit settings: Settings): Try[String] = {
    val templatePath = new Path(template)
    if (settings.storageHandler().exists(templatePath)) {
      Success(settings.storageHandler().read(templatePath))
    } else {
      Failure(new RuntimeException(s"No absolute template found for ${templatePath}."))
    }
  }

  def loadTemplateFromAppPath(template: String)(implicit settings: Settings): Try[String] = {
    val appPath = new Path(EXTERNAL_TEMPLATE_BASE_PATH, TEMPLATE_FOLDER + "/" + template)
    if (settings.storageHandler().exists(appPath)) {
      Success(settings.storageHandler().read(appPath))
    } else {
      Failure(new RuntimeException(s"No relative template found for ${appPath}."))
    }
  }

  def loadTemplateFromResources(
    domainTemplate: String
  ): Try[String] = {
    Resource.asString(s"$RESOURCE_TEMPLATE_FOLDER/${domainTemplate}") match {
      case Some(value) => Success(value)
      case None =>
        Failure(new RuntimeException(s"Relative template not found in for ${domainTemplate}"))
    }
  }
}
