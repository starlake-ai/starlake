package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import better.files.Resource
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

object Yml2DagTemplateLoader extends LazyLogging {

  private val JINJA_EXTENSION = ".j2"
  private val TEMPLATE_FOLDER = "templates"
  private val RESOURCE_DOMAIN_TEMPLATE_FOLDER = s"yml2dag/$TEMPLATE_FOLDER"

  def loadTemplate(templatePathname: String)(implicit settings: Settings): String = {
    loadTemplateFromAbsolutePath(templatePathname)
      .orElse(loadTemplateFromDagPath(templatePathname))
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

  def loadTemplateFromDagPath(template: String)(implicit settings: Settings): Try[String] = {
    val domainInDagPath = new Path(DatasetArea.dags, TEMPLATE_FOLDER + "/" + template)
    if (settings.storageHandler().exists(domainInDagPath)) {
      Success(settings.storageHandler().read(domainInDagPath))
    } else {
      Failure(new RuntimeException(s"No relative template found for ${domainInDagPath}."))
    }
  }

  def loadTemplateFromResources(
    domainTemplate: String
  ): Try[String] = {
    Resource.asString(s"$RESOURCE_DOMAIN_TEMPLATE_FOLDER/${domainTemplate}") match {
      case Some(value) => Success(value)
      case None =>
        Failure(new RuntimeException(s"Relative template not found in for ${domainTemplate}"))
    }
  }
}
