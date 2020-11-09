package com.ebiznext.comet.extractor.config

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import configs.syntax._
import DeltaColumnsMapping._

case class DeltaColumnsMapping(
  defaultColumn: Option[ColumnName] = None,
  deltaColumns: Map[TableName, ColumnName] = Map.empty
)

object DeltaColumnsMapping {
  type TableName = String
  type ColumnName = String
}

object Settings extends StrictLogging {

  private val config = ConfigFactory.load()

  val deltaColumns = config.get[DeltaColumnsMapping]("database-extractor").valueOrThrow { error =>
    error.messages.foreach(err => logger.error(err))
    throw new Exception(s"Failed to load config: $error")
  }

}
