package ai.starlake.extract

import ai.starlake.schema.model.{Trim, WriteMode}

case class ExtractDesc(extract: JDBCSchemas)

case class JDBCSchemas(
  jdbcSchemas: List[JDBCSchema],
  default: Option[JDBCSchema] = None,
  output: Option[FileFormat] = None,
  connectionRef: Option[String] = None,
  auditConnectionRef: Option[String] = None,
  fetchSize: Option[Int] = None
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
    if (default.isDefined) {
      this.copy(jdbcSchemas = jdbcSchemas.map(schema => {
        schema
          .copy(
            catalog = schema.catalog.orElse(default.flatMap(_.catalog)),
            schema =
              if (schema.schema.isEmpty) default.map(_.schema).getOrElse(schema.schema)
              else schema.schema,
            tableRemarks = schema.tableRemarks.orElse(default.flatMap(_.tableRemarks)),
            columnRemarks = schema.columnRemarks.orElse(default.flatMap(_.columnRemarks)),
            tableTypes =
              if (schema.tableTypes.isEmpty)
                default
                  .map(_.tableTypes)
                  .getOrElse(schema.tableTypes)
              else schema.tableTypes,
            template = schema.template.orElse(default.flatMap(_.template)),
            write = schema.write.orElse(default.flatMap(_.write)),
            pattern = schema.pattern.orElse(default.flatMap(_.pattern)),
            numericTrim = schema.numericTrim.orElse(default.flatMap(_.numericTrim)),
            partitionColumn = schema.partitionColumn.orElse(default.flatMap(_.partitionColumn)),
            numPartitions = schema.numPartitions.orElse(default.flatMap(_.numPartitions)),
            connectionOptions =
              if (schema.connectionOptions.isEmpty)
                default.map(_.connectionOptions).getOrElse(schema.connectionOptions)
              else schema.connectionOptions,
            fetchSize = schema.fetchSize.orElse(default.flatMap(_.fetchSize)),
            stringPartitionFunc =
              schema.stringPartitionFunc.orElse(default.flatMap(_.stringPartitionFunc)),
            fullExport = schema.fullExport.orElse(default.flatMap(_.fullExport)),
            sanitizeName = schema.sanitizeName.orElse(default.flatMap(_.sanitizeName))
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
  exclude: List[String] = Nil,
  tableTypes: List[String] = Nil,
  template: Option[String] = None,
  write: Option[WriteMode] = None,
  pattern: Option[String] = None,
  numericTrim: Option[Trim] = None,
  partitionColumn: Option[String] = None,
  numPartitions: Option[Int] = None,
  connectionOptions: Map[String, String] = Map.empty,
  fetchSize: Option[Int] = None,
  stringPartitionFunc: Option[String] = None,
  fullExport: Option[Boolean] = None,
  sanitizeName: Option[Boolean] = None
) {
  def this() = this(None) // Should never be called. Here for Jackson deserialization only

  def writeMode(): WriteMode = this.write.getOrElse(WriteMode.OVERWRITE)

  def fillWithDefaultValues(): JDBCSchema = {
    copy(
      tableTypes = if (tableTypes.isEmpty) JDBCSchema.defaultTableTypes else tableTypes,
      fullExport = if (fullExport.isEmpty) Some(false) else fullExport,
      sanitizeName = if (sanitizeName.isEmpty) Some(false) else sanitizeName
    )
  }
}

object JDBCSchema {
  private val defaultTableTypes = List(
    "TABLE",
    "VIEW",
    "SYSTEM TABLE",
    "GLOBAL TEMPORARY",
    "LOCAL TEMPORARY",
    "ALIAS",
    "SYNONYM"
  )
}

case class TableColumn(name: String, rename: Option[String] = None) {
  def this() = {
    this("", None)
  }
}

/** @param name
  *   : Table name (case insensitive)
  * @param columns
  *   : List of columns (case insensitive). Nil if all columns should be extracted
  */
case class JDBCTable(
  name: String,
  columns: List[TableColumn],
  partitionColumn: Option[String],
  numPartitions: Option[Int],
  connectionOptions: Map[String, String],
  fetchSize: Option[Int],
  fullExport: Option[Boolean]
) {
  def this() =
    this(
      "",
      Nil,
      None,
      None,
      Map.empty,
      None,
      None
    ) // Should never be called. Here for Jackson deserialization only
}
