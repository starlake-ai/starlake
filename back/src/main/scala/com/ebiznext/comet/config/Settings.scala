package com.ebiznext.comet.config

import com.ebiznext.comet.schema.handlers.{AirflowLauncher, LaunchHandler, SimpleLauncher}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import configs.syntax._

object Settings extends StrictLogging {

  case class Airflow(endpoint: String)

  case class Area(pending: String,
                  unresolved: String,
                  archive: String,
                  ingesting: String,
                  accepted: String,
                  rejected: String,
                  business: String
                 )

  case class Comet(datasets: String, metadata: String, archive: Boolean,
                   launcher: String,
                   analyze: Boolean, hive: Boolean,
                   area: Area,
                   airflow: Airflow) {
    def getLauncher(): LaunchHandler = launcher match {
      case "simple" => new SimpleLauncher
      case "airflow" => new AirflowLauncher
    }
  }

  /*
  area {
  pending = "pending"
  pending = ${?COMET_PENDING}
  unresolved = "unresolved"
  unresolved = ${?COMET_UNRESOLVED}
  archive = "archive"
  archive = ${?COMET_ARCHIVE}
  ingesting = "ingesting"
  ingesting = ${?COMET_INGESTING}
  accepted = "accepted"
  accepted = ${?COMET_ACCEPTED}
  rejected = "rejected"
  rejected = ${?COMET_REJECTED}
  business = "business"
  business = ${?COMET_BUSINESS}
}
   */

  val config: Config = ConfigFactory.load()
  val comet: Comet = config.extract[Comet].valueOrThrow { error =>
    error.messages.foreach(err => logger.error(err))
    throw new Exception("Failed to load config")
  }
  logger.info(s"Using Config $comet")


}
