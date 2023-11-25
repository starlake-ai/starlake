package ai.starlake.utils

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_PREFER_TIMESTAMP_NTZ
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{StructField, StructType, TimestampNTZType}

import java.sql.{Connection, SQLException}

object SparkUtils extends StrictLogging {
  def added(incoming: StructType, existing: StructType): StructType = {
    val incomingFields = incoming.fields.map(_.name).toSet
    val existingFields = existing.fields.map(_.name.toLowerCase()).toSet
    val newFields = incomingFields.filter(f => !existingFields.contains(f.toLowerCase()))
    val fields = incoming.fields.filter(f => newFields.contains(f.name))
    StructType(fields)
  }

  def dropped(incoming: StructType, existing: StructType): StructType = {
    val incomingFields = incoming.fields.map(_.name.toLowerCase()).toSet
    val existingFields = existing.fields.map(_.name).toSet
    val deletedFields = existingFields.filter(f => !incomingFields.contains(f.toLowerCase()))
    val fields = existing.fields.filter(f => deletedFields.contains(f.name))
    StructType(fields)
  }

  def alterTableDropColumnsString(fields: StructType, tableName: String): Seq[String] = {
    val dropFields = fields.map(_.name)
    dropFields.map(dropColumn => s"ALTER TABLE $tableName DROP COLUMN $dropColumn")
  }

  def alterTableAddColumnsString(allFields: StructType, tableName: String): Seq[String] = {
    allFields.fields.flatMap(alterTableAddColumnString(_, tableName))
  }

  def alterTableAddColumnString(field: StructField, tableName: String): Option[String] = {
    val addField = field.name
    val addFieldType = field.dataType
    val addJdbcType = JdbcUtils.getCommonJDBCType(addFieldType).map(_.databaseTypeDefinition)
    val nullable = if (!field.nullable) "NOT NULL" else ""
    addJdbcType.map(jdbcType => s"ALTER TABLE $tableName ADD COLUMN $addField $jdbcType $nullable")
  }

  /** Creates a table with a given schema. Updated from Spark 3.0.1
    */
  def getSchemaOption(
    conn: Connection,
    options: Map[String, String],
    table: String
  ): Option[StructType] = {
    val dialect = SparkUtils.dialect(options("url"))
    val preferTimestampNTZ =
      options
        .get(JDBC_PREFER_TIMESTAMP_NTZ)
        .map(_.toBoolean)
        .getOrElse(SQLConf.get.timestampType == TimestampNTZType)

    try {
      val statement =
        conn.prepareStatement(dialect.getSchemaQuery(table))
      try {
        Some(
          JdbcUtils.getSchema(
            statement.executeQuery(),
            dialect,
            isTimestampNTZ = preferTimestampNTZ
          )
        )
      } catch {
        case _: SQLException => None
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException => None
    }
  }

  /** Creates a table with a given schema. Updated from Spark 3.0.1
    */
  def createTable(
    conn: Connection,
    tableName: String,
    schema: StructType,
    caseSensitive: Boolean,
    options: JdbcOptionsInWrite
  ): Unit = {
    val statement = conn.createStatement
    val jdbcDialect = dialect(options.url)
    val strSchema = schemaString(schema, caseSensitive, options.url, options.createTableColumnTypes)
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val createTableOptions = options.createTableOptions
      val finalStrSchema =
        if (options.parameters.getOrElse("quoteIdentifiers", "false").toBoolean)
          strSchema
        else
          strSchema.replaceAll("\"", "")

      statement.executeUpdate(s"CREATE TABLE $tableName ($finalStrSchema) $createTableOptions")
      if (options.tableComment.nonEmpty) {
        try {
          val tableCommentQuery = jdbcDialect.getTableCommentQuery(tableName, options.tableComment)
          statement.executeUpdate(tableCommentQuery)
        } catch {
          case e: Exception =>
            logger.warn("Cannot create JDBC table comment. The table comment will be ignored.")
        }
      }
    } finally {
      statement.close()
    }
  }

  def isFlat(fields: StructType): Boolean = {
    val deep = fields.fields.exists(_.dataType.isInstanceOf[StructType])
    !deep
  }

  def dialect(url: String): JdbcDialect = {
    val urlForRedshift = url.replace("jdbc:redshift", "jdbc:postgresql")
    val jdbcDialect = JdbcDialects.get(urlForRedshift)
    logger.info(s"JDBC dialect $jdbcDialect")
    jdbcDialect
  }

  def schemaString(
    schema: StructType,
    caseSensitive: Boolean,
    url: String,
    createTableColumnTypes: Option[String] = None
  ) = {
    val urlForRedshift = url.replace("jdbc:redshift", "jdbc:postgresql")
    JdbcUtils.schemaString(schema, caseSensitive, urlForRedshift, createTableColumnTypes)
  }

}
