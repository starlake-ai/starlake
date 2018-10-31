package com.ebiznext.comet.config

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

object Settings extends StrictLogging {

  case class Comet(env: String)

  val config: Config = ConfigFactory.load()
  val comet = pureconfig.loadConfig[Comet] match {
    case Left(value) =>
      throw new Exception("")
    case Right(value) =>
      value
  }
}
