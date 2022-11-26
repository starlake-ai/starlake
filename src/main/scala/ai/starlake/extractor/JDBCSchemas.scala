package ai.starlake.extractor

import ai.starlake.schema.model.WriteMode

case class JDBCSchemas(
  jdbcSchemas: List[JDBCSchema],
  connectionRef: Option[String] = None,
  connection: Map[String, String] = Map.empty
)

/** @param connectionRef
  *   : JDBC Configuration to use as defined in the connection section in the application.conf
  * @param catalog
  *   : Database catalog name, optional.
  * @param schema
  *   : Database schema to use, required.
  * @param tables
  *   : Tables to extract. Nil if all tables should be extracted
  * @param tableTypes
  *   : Table types to extract
  */

case class JDBCSchema(
  catalog: Option[String] = None,
  schema: String = "",
  tableRemarks: Option[String] = None,
  columnRemarks: Option[String] = None,
  tables: List[JDBCTable] = Nil,
  tableTypes: List[String] = List(
    "TABLE",
    "VIEW",
    "SYSTEM TABLE",
    "GLOBAL TEMPORARY",
    "LOCAL TEMPORARY",
    "ALIAS",
    "SYNONYM"
  ),
  template: Option[String] = None,
  write: Option[WriteMode] = None,
  pattern: Option[String] = None
) {
  def this() = this(None) // Should never be called. Here for Jackson deserialization only
}

/** @param name
  *   : Table name (case insensitive)
  * @param columns
  *   : List of columns (case insensitive). Nil if all columns should be extracted
  */
case class JDBCTable(name: String, columns: List[String]) {
  def this() = this("", Nil) // Should never be called. Here for Jackson deserialization only
}
