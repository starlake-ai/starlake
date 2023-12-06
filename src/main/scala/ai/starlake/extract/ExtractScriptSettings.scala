package ai.starlake.extract

import ai.starlake.extract.DeltaColumnsMapping._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import pureconfig._
import pureconfig.generic.auto._

case class DeltaColumnsMapping(
  defaultColumn: Option[ColumnName] = None,
  deltaColumns: Map[TableName, ColumnName] = Map.empty
)

object DeltaColumnsMapping {
  private type TableName = String
  private type ColumnName = String
}

object ExtractScriptSettings extends StrictLogging {

  private val config = ConfigFactory.load()

  val deltaColumns =
    ConfigSource
      .fromConfig(config)
      .at("database-extractor")
      .load[DeltaColumnsMapping] match {
      case Right(b) => b
      case _        => DeltaColumnsMapping()
    }
}
