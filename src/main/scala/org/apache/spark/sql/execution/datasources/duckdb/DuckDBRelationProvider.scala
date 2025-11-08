package org.apache.spark.sql.execution.datasources.duckdb

import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.execution.datasources.jdbc.{
  JdbcOptionsInWrite,
  JdbcRelationProvider,
  JdbcUtils
}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DuckDBRelationProvider extends JdbcRelationProvider {
  override def shortName(): String = "starlake-duckdb"
  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    df: DataFrame
  ): BaseRelation = {
    val options = new JdbcOptionsInWrite(parameters)
    val isCaseSensitive = sqlContext.sparkSession.sessionState.conf.caseSensitiveAnalysis
    val dialect = JdbcDialects.get(options.url)
    val conn = dialect.createConnectionFactory(options)(-1)
    var saveTableAfterConnectionHasBeenClosed = false
    var tableSchema: Option[StructType] = None
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTableAfterConnectionHasBeenClosed = true
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, options.table, df.schema, isCaseSensitive, options)
              tableSchema = Some(df.schema)
              saveTableAfterConnectionHasBeenClosed = true
            }

          case SaveMode.Append =>
            tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTableAfterConnectionHasBeenClosed = true

          case SaveMode.ErrorIfExists =>
            throw QueryCompilationErrors.tableOrViewAlreadyExistsError(options.table)

          case SaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, options.table, df.schema, isCaseSensitive, options)
        tableSchema = Some(df.schema)
        saveTableAfterConnectionHasBeenClosed = true
      }
    } finally {
      conn.close()
    }
    if (saveTableAfterConnectionHasBeenClosed)
      saveTable(df, tableSchema, isCaseSensitive, options)

    createRelation(sqlContext, parameters)
  }
}
