package ai.starlake.schema.generator.yml2dag

import ai.starlake.config.{DatasetArea, Settings}
import better.files.Resource
import org.apache.hadoop.fs.Path

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object Yml2DagTemplateLoader {

  private val JINJA_EXTENSION = ".j2"
  private val TEMPLATE_FOLDER = "templates"
  val DOMAIN_TEMPLATE_FOLDER = s"$TEMPLATE_FOLDER/domains"
  private val RESOURCE_DOMAIN_TEMPLATE_FOLDER = s"yml2dag/$DOMAIN_TEMPLATE_FOLDER"

  def loadTemplate(template: Yml2DagTemplate)(implicit settings: Settings): Try[String] = {
    loadTemplateFromAbsolutePath(template)
      .orElse(loadTemplateFromDagPath(template))
      .orElse(loadTemplateFromResources(template))
      .orElse(
        Failure(
          new RuntimeException(
            s"Template `${template.path}` not found. Please provide an absolute path or a template returned by `list` command."
          )
        )
      )
  }

  def loadTemplateFromAbsolutePath(
    template: Yml2DagTemplate
  )(implicit settings: Settings): Try[String] = {
    val templatePath = new Path(template.path)
    if (settings.storageHandler.exists(templatePath)) {
      Success(settings.storageHandler.read(templatePath))
    } else {
      Failure(new RuntimeException(s"No absolute template found for ${templatePath}."))
    }
  }

  def loadTemplateFromDagPath(
    template: Yml2DagTemplate
  )(implicit settings: Settings): Try[String] = {
    val pathToResolve = template match {
      case DomainTemplate(path) if !path.endsWith(JINJA_EXTENSION) =>
        path + JINJA_EXTENSION
      case DomainTemplate(path) => path
    }
    val domainInDagPath = new Path(DatasetArea.dags, DOMAIN_TEMPLATE_FOLDER + "/" + pathToResolve)
    if (settings.storageHandler.exists(domainInDagPath)) {
      Success(settings.storageHandler.read(domainInDagPath))
    } else {
      Failure(new RuntimeException(s"No relative template found for ${domainInDagPath}."))
    }
  }

  def listTemplateFromDagPath()(implicit settings: Settings): List[DomainTemplate] = {
    if (settings.storageHandler.exists(DatasetArea.dags))
      settings.storageHandler
        .list(
          DatasetArea.dags,
          JINJA_EXTENSION,
          recursive = true,
          sortByName = true
        )
        .map(p =>
          p.toUri.getPath.substring(
            DatasetArea.dags.toUri.getPath.length + DOMAIN_TEMPLATE_FOLDER.length + 2,
            p.toUri.getPath.length - JINJA_EXTENSION.length
          )
        )
        .map(DomainTemplate)
    else
      Nil
  }

  def loadTemplateFromResources(
    domainTemplate: Yml2DagTemplate
  ): Try[String] = {
    val templateToResolve = domainTemplate match {
      case DomainTemplate(path) if path.endsWith(JINJA_EXTENSION) => path
      case DomainTemplate(path)                                   => path
    }
    Resource.asString(s"$RESOURCE_DOMAIN_TEMPLATE_FOLDER/${templateToResolve}") match {
      case Some(value) => Success(value)
      case None =>
        Failure(new RuntimeException(s"Relative template not found in for ${domainTemplate}"))
    }
  }

  def listTemplateFromResources(): List[DomainTemplate] = {
    val domainFolder = Resource.getUrl(s"$RESOURCE_DOMAIN_TEMPLATE_FOLDER").toURI
    val domainFolderPath = Paths.get(domainFolder)
    val walk = Files.walk(domainFolderPath)
    walk
      .iterator()
      .asScala
      .map(_.toString)
      .filter(_.endsWith(JINJA_EXTENSION))
      .map(p => p.substring(domainFolder.getPath.length + 1, p.length - JINJA_EXTENSION.length))
      .map(DomainTemplate)
      .toList
  }
}
