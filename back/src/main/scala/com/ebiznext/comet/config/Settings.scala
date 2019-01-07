package com.ebiznext.comet.config

import com.ebiznext.comet.schema.handlers.{AirflowLauncher, LaunchHandler, SimpleLauncher}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import configs.syntax._

object Settings extends StrictLogging {

  case class Airflow(endpoint: String)

  case class Comet(datasets: String, metadata: String, archive: Boolean, launcher: String, analyze: Boolean, hive: Boolean, airflow: Airflow) {
    def getLauncher(): LaunchHandler = launcher match {
      case "simple" => new SimpleLauncher
      case "airflow" => new AirflowLauncher
    }
  }

  val config: Config = ConfigFactory.load()
  val comet: Comet = config.extract[Comet].valueOrThrow { error =>
    error.messages.foreach(err => logger.error(err))
    throw new Exception("Failed to load config")
  }
  logger.info(s"Using Config $comet")
}
