package ai.starlake.extract

import ai.starlake.schema.model.WriteMode

case class JDBCSchemas(
  jdbcSchemas: List[JDBCSchema],
  globalJdbcSchema: Option[JDBCSchema] = None,
  connectionRef: Option[String] = None,
  connection: Map[String, String] = Map.empty
) {

  /** @return
    *   jdbc schemas filled with global jdbc schemas attributes if empty. Considered attributes are
    *   all except tables:
    *   - catalog
    *   - schema
    *   - tableRemarks
    *   - columnRemarks
    *   - tableTypes
    *   - template
    *   - write
    *   - pattern
    */
  def propageGlobalJdbcSchemas(): JDBCSchemas = {
    if (globalJdbcSchema.isDefined) {
      this.copy(jdbcSchemas = jdbcSchemas.map(schema => {
        schema
          .copy(
            catalog = schema.catalog.orElse(globalJdbcSchema.flatMap(_.catalog)),
            schema =
              if (schema.schema.isEmpty) globalJdbcSchema.map(_.schema).getOrElse(schema.schema)
              else schema.schema,
            tableRemarks = schema.tableRemarks.orElse(globalJdbcSchema.flatMap(_.tableRemarks)),
            columnRemarks = schema.columnRemarks.orElse(globalJdbcSchema.flatMap(_.columnRemarks)),
            tableTypes =
              if (schema.tableTypes.isEmpty)
                globalJdbcSchema
                  .map(_.tableTypes)
                  .getOrElse(schema.tableTypes)
              else schema.tableTypes,
            template = schema.template.orElse(globalJdbcSchema.flatMap(_.template)),
            write = schema.write.orElse(globalJdbcSchema.flatMap(_.write)),
            pattern = schema.pattern.orElse(globalJdbcSchema.flatMap(_.pattern))
          )
          .fillWithDefaultValues()
      }))
    } else {
      this
    }
  }
}

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
  tableTypes: List[String] = Nil,
  template: Option[String] = None,
  write: Option[WriteMode] = None,
  pattern: Option[String] = None
) {
  def this() = this(None) // Should never be called. Here for Jackson deserialization only

  def fillWithDefaultValues() = {
    copy(
      tableTypes = if (tableTypes.isEmpty) JDBCSchema.defaultTableTypes else tableTypes
    )
  }
}

object JDBCSchema {
  val defaultTableTypes = List(
    "TABLE",
    "VIEW",
    "SYSTEM TABLE",
    "GLOBAL TEMPORARY",
    "LOCAL TEMPORARY",
    "ALIAS",
    "SYNONYM"
  )
}

/** @param name
  *   : Table name (case insensitive)
  * @param columns
  *   : List of columns (case insensitive). Nil if all columns should be extracted
  */
case class JDBCTable(name: String, columns: List[String]) {
  def this() = this("", Nil) // Should never be called. Here for Jackson deserialization only
}
