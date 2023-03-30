package ai.starlake.schema.generator.yml2dag.command

import ai.starlake.config.Settings
import ai.starlake.schema.generator.yml2dag.{DomainTemplate, Yml2DagTemplateLoader}
import ai.starlake.schema.generator.yml2dag.config.Yml2DagShowConfig
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

class Yml2DagShowCommand() extends LazyLogging {

  def run(yml2DagShowConfig: Yml2DagShowConfig): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    yml2DagShowConfig.domainTemplatePath match {
      case Some(domainTemplatePath) =>
        showDomainDag(domainTemplatePath) match {
          case Failure(exception) => logger.error(exception.getMessage, exception)
          case Success(_)         => //
        }
      case None => // Do nothing
    }
  }

  private def showDomainDag(
    domainTemplatePath: String
  )(implicit settings: Settings): Try[Unit] = {
    for {
      dagTemplate <- Yml2DagTemplateLoader.loadTemplate(DomainTemplate(domainTemplatePath))
    } yield {
      logger.info(s"Template $domainTemplatePath is \n${dagTemplate}")
    }
  }
}
