package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model.{StarlakeDataType, StarlakeField, StarlakeSchema}
import com.typesafe.scalalogging.LazyLogging

import java.sql.{Connection, ResultSetMetaData, SQLException}
import java.util.regex.Pattern
import scala.collection.immutable.TreeMap

/** Pure SQL DDL string generation methods using engine-independent StarlakeSchema types. This
  * replaces SparkDDLBuilder by removing the dependency on Spark's StructType.
  */
object DDLBuilder extends LazyLogging {

  case class JdbcTypeMapping(databaseTypeDefinition: String, sqlType: Int)

  def added(incoming: StarlakeSchema, existing: StarlakeSchema): StarlakeSchema = {
    val existingFields = existing.fields.map(_.name.toLowerCase()).toSet
    val newFields = incoming.fields.filter(f => !existingFields.contains(f.name.toLowerCase()))
    StarlakeSchema(newFields)
  }

  def dropped(incoming: StarlakeSchema, existing: StarlakeSchema): StarlakeSchema = {
    val incomingFields = incoming.fields.map(_.name.toLowerCase()).toSet
    val deletedFields = existing.fields.filter(f => !incomingFields.contains(f.name.toLowerCase()))
    StarlakeSchema(deletedFields)
  }

  def alterTableDropColumnsString(
    engineName: String,
    fields: StarlakeSchema,
    tableName: String
  ): Seq[String] = {
    val dropFields = fields.fieldNames
    val ifExists = if (engineName.toLowerCase() == "redshift") "" else "IF EXISTS"
    dropFields.map(dropColumn => s"ALTER TABLE $tableName DROP COLUMN $ifExists $dropColumn")
  }

  def alterTableAddColumnsString(
    engineName: String,
    allFields: StarlakeSchema,
    tableName: String,
    attributesWithDDLType: Map[String, String]
  ): Seq[String] = {
    allFields.fields
      .flatMap(alterTableAddColumnString(engineName, _, tableName, attributesWithDDLType))
  }

  def alterTableAddColumnString(
    engineName: String,
    field: StarlakeField,
    tableName: String,
    attributesWithDDLType: Map[String, String]
  ): Option[String] = {
    val addField = field.name
    val addFieldType = field.dataType

    val (jdbcType, isArray) = getJdbcTypeMapping(addFieldType)
    val typeStr = jdbcType.map(_.databaseTypeDefinition)
    val withArray =
      if (isArray) {
        engineName match {
          case "snowflake" => typeStr.map(_ => "ARRAY")
          case "duckdb"    => typeStr.map(_ + "[]")
          case _           => typeStr
        }
      } else
        typeStr
    val addJdbcType =
      attributesWithDDLType
        .get(addField)
        .orElse(withArray)

    val nullable = ""
    val ifNotExists =
      if (engineName.toLowerCase() == "redshift") "" else "IF NOT EXISTS"
    addJdbcType.map(jdbcType =>
      s"ALTER TABLE $tableName ADD COLUMN $ifNotExists $addField $jdbcType $nullable"
    )
  }

  def isFlat(fields: StarlakeSchema): Boolean = {
    !fields.fields.exists(_.dataType.isInstanceOf[StarlakeDataType.SLStruct])
  }

  /** Retrieve the schema of an existing JDBC table as a StarlakeSchema, using JDBC metadata
    * instead of Spark's JdbcDialect.
    */
  def getSchemaOption(
    conn: Connection,
    options: Map[String, String],
    table: String
  ): Option[StarlakeSchema] = {
    try {
      val statement = conn.prepareStatement(s"SELECT * FROM $table WHERE 1=0")
      try {
        val rs = statement.executeQuery()
        val meta = rs.getMetaData
        val fields = (1 to meta.getColumnCount).map { i =>
          val name = meta.getColumnName(i)
          val nullable = meta.isNullable(i) != ResultSetMetaData.columnNoNulls
          val dataType = jdbcSqlTypeToSL(meta.getColumnType(i), meta.getPrecision(i), meta.getScale(i))
          StarlakeField(name, dataType, nullable, None)
        }
        Some(StarlakeSchema(fields))
      } catch {
        case _: SQLException => None
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException => None
    }
  }

  private def jdbcSqlTypeToSL(sqlType: Int, precision: Int, scale: Int): StarlakeDataType = {
    sqlType match {
      case java.sql.Types.BIT | java.sql.Types.BOOLEAN => StarlakeDataType.SLBoolean
      case java.sql.Types.TINYINT                       => StarlakeDataType.SLByte
      case java.sql.Types.SMALLINT                      => StarlakeDataType.SLShort
      case java.sql.Types.INTEGER                       => StarlakeDataType.SLInt
      case java.sql.Types.BIGINT                        => StarlakeDataType.SLLong
      case java.sql.Types.FLOAT | java.sql.Types.REAL   => StarlakeDataType.SLFloat
      case java.sql.Types.DOUBLE                        => StarlakeDataType.SLDouble
      case java.sql.Types.NUMERIC | java.sql.Types.DECIMAL =>
        if (precision > 0) StarlakeDataType.SLDecimal(precision, scale)
        else StarlakeDataType.SLDecimal()
      case java.sql.Types.DATE => StarlakeDataType.SLDate
      case java.sql.Types.TIME | java.sql.Types.TIME_WITH_TIMEZONE =>
        StarlakeDataType.SLTime
      case java.sql.Types.TIMESTAMP =>
        StarlakeDataType.SLTimestamp
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE =>
        // TIMESTAMP WITH TIME ZONE — keep as standard SLTimestamp
        // since Starlake's SLTimestamp already implies TZ-awareness
        StarlakeDataType.SLTimestamp
      case java.sql.Types.BINARY | java.sql.Types.VARBINARY | java.sql.Types.LONGVARBINARY |
          java.sql.Types.BLOB =>
        StarlakeDataType.SLBinary
      case _ => StarlakeDataType.SLString
    }
  }

  def updateJdbcTableSchema(
    engineName: String,
    conn: Connection,
    jdbcOptions: Map[String, String],
    domainAndTableName: String,
    incomingSchema: StarlakeSchema,
    attributesWithDDLType: Map[String, String]
  ): Unit = {
    buildUpdateJdbcTableSchemaSQL(
      engineName,
      conn,
      jdbcOptions,
      domainAndTableName,
      incomingSchema,
      attributesWithDDLType
    ).foreach(JdbcDbUtils.executeAlterTable(_, conn))
  }

  def buildUpdateJdbcTableSchemaSQL(
    engineName: String,
    conn: Connection,
    jdbcOptions: Map[String, String],
    domainAndTableName: String,
    incomingSchema: StarlakeSchema,
    attributesWithDDLType: Map[String, String]
  ): Seq[String] = {
    if (isFlat(incomingSchema)) {
      val existingSchema = getSchemaOption(conn, jdbcOptions, domainAndTableName)
      val addedColumns = added(incomingSchema, existingSchema.getOrElse(incomingSchema))
      val deletedColumns = dropped(incomingSchema, existingSchema.getOrElse(incomingSchema))
      val alterTableDropColumns =
        alterTableDropColumnsString(engineName, deletedColumns, domainAndTableName)
      if (alterTableDropColumns.nonEmpty) {
        logger.info(
          s"alter table $domainAndTableName with ${alterTableDropColumns.size} columns to drop"
        )
        logger.debug(s"alter table ${alterTableDropColumns.mkString("\n")}")
      }
      val alterTableAddColumns =
        alterTableAddColumnsString(
          engineName,
          addedColumns,
          domainAndTableName,
          attributesWithDDLType
        )
      if (alterTableAddColumns.nonEmpty) {
        logger.info(
          s"alter table $domainAndTableName with ${alterTableAddColumns.size} columns to add"
        )
        logger.debug(s"alter table ${alterTableAddColumns.mkString("\n")}")
      }
      alterTableDropColumns ++ alterTableAddColumns
    } else
      Seq.empty
  }

  def createTable(
    engineName: String,
    conn: Connection,
    domainAndTableName: String,
    schema: StarlakeSchema,
    caseSensitive: Boolean,
    temporaryTable: Boolean,
    options: Map[String, String],
    attrDdlMapping: Map[String, Map[String, String]]
  )(implicit settings: Settings): Unit = {
    val (createSchemaSql, createTableSql, commentSql) =
      buildCreateTableSQL(
        engineName,
        domainAndTableName,
        schema,
        caseSensitive,
        temporaryTable,
        options,
        attrDdlMapping
      )
    val statement = conn.createStatement
    try {
      logger.info(s"Creating schema $createSchemaSql")
      statement.executeUpdate(createSchemaSql)
      logger.info(s"Creating table $createTableSql")
      statement.executeUpdate(createTableSql)
      commentSql.foreach { comment =>
        try {
          logger.info(s"Creating table comment $comment")
          statement.executeUpdate(comment)
        } catch {
          case _: Exception =>
            logger.warn(s"Cannot create JDBC table comment on $domainAndTableName. Skipping it.")
        }
      }
    } finally {
      statement.close()
    }
  }

  def buildCreateTableSQL(
    engineName: String,
    domainAndTableName: String,
    schema: StarlakeSchema,
    caseSensitive: Boolean,
    temporaryTable: Boolean,
    options: Map[String, String],
    attrDdlMapping: Map[String, Map[String, String]]
  )(implicit settings: Settings): (String, String, Option[String]) = {
    val url = options.getOrElse("url", "")
    val strSchema =
      sqlSchemaString(schema, caseSensitive, url, attrDdlMapping, 0)
    val createTableOptions = options.getOrElse("createTableOptions", "")
    val finalStrSchema =
      if (options.getOrElse("quoteIdentifiers", "false").toBoolean)
        strSchema
      else
        strSchema.replaceAll("\"", "")

    val domainName = domainAndTableName.split('.').head
    val createSchemaSQL =
      if (domainName != "__ignore__")
        s"CREATE SCHEMA IF NOT EXISTS $domainName"
      else
        ""
    val temporary = if (temporaryTable) "TEMP" else ""
    val createTableSQL =
      s"CREATE $temporary TABLE IF NOT EXISTS $domainAndTableName ($finalStrSchema) $createTableOptions"

    val tableComment = options.getOrElse("tableComment", "")
    val commentSQL =
      if (tableComment.nonEmpty)
        Some(s"COMMENT ON TABLE $domainAndTableName IS '$tableComment'")
      else
        None

    (createSchemaSQL, createTableSQL, commentSQL)
  }

  def sqlSchemaString(
    schema: StarlakeSchema,
    caseSensitive: Boolean,
    url: String,
    sparkToSqlTypeMappings: Map[String, Map[String, String]] = Map.empty,
    level: Int
  )(implicit settings: Settings): String = {
    sqlSchema(schema, caseSensitive, url, sparkToSqlTypeMappings, level).mkString(", ")
  }

  def sqlSchema(
    schema: StarlakeSchema,
    caseSensitive: Boolean,
    url: String,
    sparkToSqlTypeMappings: Map[String, Map[String, String]] = Map.empty,
    level: Int
  )(implicit settings: Settings): List[String] = {
    logger.debug(s"sqlSchema of $schema")
    val dialectPattern = Pattern
      .compile("jdbc:([a-zA-Z]+):.*")
      .matcher(url.replace(":starlake:", ":"))
    assert(dialectPattern.find(), s"Cannot determine JDBC dialect from URL: $url")
    val dialectName = dialectPattern.group(1)
    val jdbcEng = settings.appConfig.jdbcEngines(dialectName)

    val typMap: Map[String, Map[String, String]] =
      if (caseSensitive) sparkToSqlTypeMappings
      else TreeMap(sparkToSqlTypeMappings.toSeq: _*)(Ordering.by(_.toLowerCase))

    val columns =
      schema.fields.flatMap { field =>
        val nullable = if (!field.nullable && level == 0) "NOT NULL" else ""
        val description =
          if (level == 0) field.comment.map(d => s"COMMENT '$d'").getOrElse("")
          else ""
        val ddlTyp = typMap.get(field.name).flatMap(_.get(dialectName))
        val name = field.name

        val column =
          if (jdbcEng.supportsJson.getOrElse(false)) { // DuckDB only
            val (elementType, repeated) =
              field.dataType match {
                case StarlakeDataType.SLArray(et) => (et, true)
                case _                            => (field.dataType, false)
              }
            val element =
              elementType match {
                case StarlakeDataType.SLStruct(structFields) =>
                  val fields =
                    sqlSchema(
                      StarlakeSchema(structFields),
                      caseSensitive,
                      url,
                      sparkToSqlTypeMappings,
                      level + 1
                    )
                  if (repeated) s"$name STRUCT(${fields.mkString(",")})[]"
                  else s"$name STRUCT(${fields.mkString(",")})"
                case StarlakeDataType.SLDecimal(precision, scale) =>
                  if (repeated) s"$name DECIMAL($precision,$scale)[]"
                  else s"$name DECIMAL($precision,$scale) $nullable"
                case _ =>
                  val (mapping, _) = getJdbcTypeMapping(field.dataType)
                  val typ = ddlTyp.getOrElse(
                    mapping.map(_.databaseTypeDefinition).getOrElse("TEXT")
                  )
                  if (typ.endsWith("[]")) s"$name $typ"
                  else if (repeated) s"$name $typ[]"
                  else s"$name $typ $nullable"
              }
            Some(element)
          } else {
            if (
              field.dataType.isInstanceOf[StarlakeDataType.SLStruct] ||
              field.dataType.isInstanceOf[StarlakeDataType.SLArray]
            ) {
              throw new IllegalArgumentException(
                "Array and nested struct types are not supported in the schema for this database"
              )
            }
            val (mapping, _) = getJdbcTypeMapping(field.dataType)
            val typ = ddlTyp.getOrElse(
              mapping.map(_.databaseTypeDefinition).getOrElse("TEXT")
            )
            Some(s"$name $typ")
          }
        column
      }
    columns.toList
  }

  def getJdbcTypeMapping(dt: StarlakeDataType): (Option[JdbcTypeMapping], Boolean) = {
    val (elementType, isArray) =
      dt match {
        case StarlakeDataType.SLArray(elementType) => (elementType, true)
        case _                                     => (dt, false)
      }
    val jdbcType =
      elementType match {
        case StarlakeDataType.SLInt =>
          Some(JdbcTypeMapping("INTEGER", java.sql.Types.INTEGER))
        case StarlakeDataType.SLLong =>
          Some(JdbcTypeMapping("BIGINT", java.sql.Types.BIGINT))
        case StarlakeDataType.SLDouble =>
          Some(JdbcTypeMapping("DOUBLE PRECISION", java.sql.Types.DOUBLE))
        case StarlakeDataType.SLFloat =>
          Some(JdbcTypeMapping("REAL", java.sql.Types.FLOAT))
        case StarlakeDataType.SLShort =>
          Some(JdbcTypeMapping("INTEGER", java.sql.Types.SMALLINT))
        case StarlakeDataType.SLByte =>
          Some(JdbcTypeMapping("BYTE", java.sql.Types.TINYINT))
        case StarlakeDataType.SLBoolean =>
          Some(JdbcTypeMapping("BIT(1)", java.sql.Types.BIT))
        case StarlakeDataType.SLString =>
          Some(JdbcTypeMapping("TEXT", java.sql.Types.CLOB))
        case StarlakeDataType.SLVariant =>
          Some(JdbcTypeMapping("TEXT", java.sql.Types.CLOB))
        case StarlakeDataType.SLBinary =>
          Some(JdbcTypeMapping("BLOB", java.sql.Types.BLOB))
        case StarlakeDataType.SLTimestamp =>
          Some(JdbcTypeMapping("TIMESTAMP", java.sql.Types.TIMESTAMP))
        case StarlakeDataType.SLTimestampNTZ =>
          Some(JdbcTypeMapping("TIMESTAMP", java.sql.Types.TIMESTAMP))
        case StarlakeDataType.SLTime =>
          Some(JdbcTypeMapping("TIME", java.sql.Types.TIME))
        case StarlakeDataType.SLDate =>
          Some(JdbcTypeMapping("DATE", java.sql.Types.DATE))
        case StarlakeDataType.SLDecimal(precision, scale) =>
          Some(JdbcTypeMapping(s"DECIMAL($precision,$scale)", java.sql.Types.DECIMAL))
        case _ => None
      }
    (jdbcType, isArray)
  }

  /** DuckDB-specific type mapping that uses DuckDB's native type names for better precision.
    * Prefers BOOLEAN over BIT(1), VARCHAR over TEXT, TIMESTAMPTZ over TIMESTAMP, etc.
    */
  def getDuckDbTypeMapping(dt: StarlakeDataType): (Option[JdbcTypeMapping], Boolean) = {
    val (elementType, isArray) =
      dt match {
        case StarlakeDataType.SLArray(elementType) => (elementType, true)
        case _                                     => (dt, false)
      }
    val jdbcType =
      elementType match {
        case StarlakeDataType.SLInt =>
          Some(JdbcTypeMapping("INTEGER", java.sql.Types.INTEGER))
        case StarlakeDataType.SLLong =>
          Some(JdbcTypeMapping("BIGINT", java.sql.Types.BIGINT))
        case StarlakeDataType.SLDouble =>
          Some(JdbcTypeMapping("DOUBLE", java.sql.Types.DOUBLE))
        case StarlakeDataType.SLFloat =>
          Some(JdbcTypeMapping("FLOAT", java.sql.Types.FLOAT))
        case StarlakeDataType.SLShort =>
          Some(JdbcTypeMapping("SMALLINT", java.sql.Types.SMALLINT))
        case StarlakeDataType.SLByte =>
          Some(JdbcTypeMapping("TINYINT", java.sql.Types.TINYINT))
        case StarlakeDataType.SLBoolean =>
          Some(JdbcTypeMapping("BOOLEAN", java.sql.Types.BOOLEAN))
        case StarlakeDataType.SLString =>
          Some(JdbcTypeMapping("VARCHAR", java.sql.Types.VARCHAR))
        case StarlakeDataType.SLVariant =>
          Some(JdbcTypeMapping("JSON", java.sql.Types.VARCHAR))
        case StarlakeDataType.SLBinary =>
          Some(JdbcTypeMapping("BLOB", java.sql.Types.BLOB))
        case StarlakeDataType.SLTimestamp =>
          Some(JdbcTypeMapping("TIMESTAMPTZ", java.sql.Types.TIMESTAMP_WITH_TIMEZONE))
        case StarlakeDataType.SLTimestampNTZ =>
          Some(JdbcTypeMapping("TIMESTAMP", java.sql.Types.TIMESTAMP))
        case StarlakeDataType.SLTime =>
          Some(JdbcTypeMapping("TIME", java.sql.Types.TIME))
        case StarlakeDataType.SLDate =>
          Some(JdbcTypeMapping("DATE", java.sql.Types.DATE))
        case StarlakeDataType.SLDecimal(precision, scale) =>
          Some(JdbcTypeMapping(s"DECIMAL($precision,$scale)", java.sql.Types.DECIMAL))
        case _ => None
      }
    (jdbcType, isArray)
  }
}