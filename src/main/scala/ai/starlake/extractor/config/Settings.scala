package ai.starlake.extractor.config

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

  val deltaColumns =
    config.get[DeltaColumnsMapping]("database-extractor").valueOrElse(DeltaColumnsMapping())

}
