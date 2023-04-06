package ai.starlake.schema.generator.yml2dag.command

import ai.starlake.config.Settings
import ai.starlake.schema.generator.yml2dag.Yml2DagTemplateLoader
import ai.starlake.schema.generator.yml2dag.config.Yml2DagListConfig
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

class Yml2DagListCommand() extends LazyLogging {

  def run(yml2DagListConfig: Yml2DagListConfig): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    listDomainDag() match {
      case Failure(exception) => logger.error(exception.getMessage, exception)
      case Success(_)         => //
    }
  }

  private def listDomainDag()(implicit settings: Settings): Try[Unit] = {
    val templatesInDagPath = Yml2DagTemplateLoader.listTemplateFromDagPath().map(_.path)
    val templatesFromResource = Yml2DagTemplateLoader.listTemplateFromResources().map(_.path)
    val templatesInDagPathFormatted = templatesInDagPath match {
      case Nil => "No template found"
      case l   => l.mkString("\n\t")
    }
    val templatesFromResourceFormatted = templatesFromResource.mkString("\n\t")
    logger.info(s"""
        | Available dag templates
        |
        | In dags/${Yml2DagTemplateLoader.DOMAIN_TEMPLATE_FOLDER} folder :
        | \t$templatesInDagPathFormatted
        |
        | Starlake :
        | \t$templatesFromResourceFormatted
        |""".stripMargin)
    Success(())
  }
}
