package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.utils.Formatter.RichFormatter
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.sql.Timestamp

class JdbcTableInfo {
  def extractLastModifiedTime(config: TablesExtractConfig)(implicit
    settings: Settings
  ): List[(String, List[(String, Long)])] = {
    val conn = ConnectionInfo.getConnectionOrDefault(config.connectionRef)

    val lastModifiedQuery =
      settings.appConfig
        .jdbcEngines(conn.getJdbcEngineName().toString)
        .lastModifiedQuery
        .getOrElse(
          throw new IllegalArgumentException(
            s"Last modified query not defined for engine ${conn.getJdbcEngineName()}"
          )
        )

    val finalQueries =
      config.tables.toList.map { case (schema, tables) =>
        val tablesInSet = tables.map(_.toLowerCase).mkString("'", "','", "'")
        val query = lastModifiedQuery.richFormat(
          Map(
            "table"  -> tablesInSet,
            "schema" -> schema
          ),
          Map.empty
        )
        (schema, query)
      }

    val lastModifiedTimes =
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), conn.options) {
        connection =>
          finalQueries.map { case (schema, finalQuery) =>
            // Execute the query and return the result as a Map
            val rows =
              JdbcDbUtils.executeQueryAsMap(finalQuery, connection)
            val tableTimes =
              rows.map { row =>
                val noCaseRow = CaseInsensitiveMap(row)
                val tableName = noCaseRow("table_name")
                val lastModified = Timestamp.valueOf(noCaseRow("last_altered")).getTime
                (tableName, lastModified)
              }
            (schema, tableTimes)
          }
      }
    lastModifiedTimes

  }
}
