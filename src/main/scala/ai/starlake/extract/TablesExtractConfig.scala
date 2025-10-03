package ai.starlake.extract

import ai.starlake.schema.model.WriteMode

case class TablesExtractConfig(
  writeMode: Option[WriteMode] = None,
  connectionRef: Option[String] = None,
  tables: Map[String, List[String]] = Map.empty,
  database: Option[String] = None,
  persist: Boolean = true,
  external: Boolean = false
)
