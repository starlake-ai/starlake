package com.ebiznext.comet.database.extractor.config

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import configs.syntax._

case class DeltaColumnsMapping(
  defaultColumn: Option[String] = None,
  deltaColumns: Map[String, String] = Map.empty
)

object Settings extends StrictLogging {

  private val config = ConfigFactory.load()
  val deltaColumns = config.get[DeltaColumnsMapping]("database-extractor").valueOrThrow { error =>
    error.messages.foreach(err => logger.error(err))
    throw new Exception(s"Failed to load config: $error")
  }

}
