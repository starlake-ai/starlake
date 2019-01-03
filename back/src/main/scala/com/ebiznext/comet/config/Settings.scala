package com.ebiznext.comet.config

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

object Settings extends StrictLogging {

  case class Airflow(endpoint: String)

  case class Comet(datasets: String, metadata: String, staging: Boolean, airflow: Airflow)

  val config: Config = ConfigFactory.load()
  //  val comet = pureconfig.loadConfig[Comet] match {
  //    case Left(value) =>
  //      value.toList.foreach(f => println(f.description))
  //      throw new Exception("")
  //    case Right(value) =>
  //      value
  //  }

  val airflow = Airflow(config.getString("airflow.endpoint"))
  val comet = Comet(
    config.getString("datasets"),
    config.getString("metadata"),
    config.getBoolean("staging"),
    airflow
  )
}
