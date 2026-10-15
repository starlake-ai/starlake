package ai.starlake.serve.catalog

import ai.starlake.config.ConnectionInfo
import ai.starlake.extract.JdbcDbUtils

import java.sql.{Connection, DatabaseMetaData, ResultSet, Types}
import scala.collection.mutable.ListBuffer
import scala.util.Using
import scala.util.control.NonFatal

class JdbcCatalogBackend(connectionInfo: ConnectionInfo) extends CatalogBackend {

  /** Marks a failure raised after the connection was opened. */
  private final class QueryFailure(cause: Throwable) extends Exception(cause)

  private def scrubbed(e: Throwable): String =
    CatalogScrubber.scrub(CatalogJson.messageOf(e), connectionInfo.options)

  private[catalog] def withConnection[T](f: Connection => T): T =
    try {
      JdbcDbUtils.withJDBCConnection(None, connectionInfo.options) { conn =>
        try f(conn)
        catch { case NonFatal(e) => throw new QueryFailure(e) }
      }
    } catch {
      case e: CatalogException => throw e
      case q: QueryFailure =>
        q.getCause match {
          case c: CatalogException => throw c
          case cause => throw new CatalogException(CatalogErrorCode.QueryFailed, scrubbed(cause))
        }
      case NonFatal(e) => throw new CatalogException(CatalogErrorCode.ConnectionFailed, scrubbed(e))
    }

  private def catalogOf(conn: Connection): String =
    Option(connectionInfo.getCatalog()).getOrElse(conn.getCatalog)

  private def collect[T](rs: ResultSet)(f: ResultSet => Option[T]): List[T] =
    Using.resource(rs) { rs =>
      val buffer = ListBuffer[T]()
      while (rs.next()) f(rs).foreach(buffer += _)
      buffer.toList
    }

  private def nonEmpty(s: String): Option[String] = Option(s).map(_.trim).filter(_.nonEmpty)

  override def schemas(): List[CatalogSchema] = withConnection { conn =>
    collect(conn.getMetaData.getSchemas(catalogOf(conn), null)) { rs =>
      Option(rs.getString("TABLE_SCHEM"))
    }.filterNot(_.equalsIgnoreCase("INFORMATION_SCHEMA"))
      .distinct
      .sorted
      .map(CatalogSchema.apply)
  }

  /** Drivers name table types differently (DuckDB: "BASE TABLE"), so the call is unfiltered and
    * types are classified here. System objects, indexes and sequences are dropped.
    */
  private[catalog] def kindOf(tableType: String): Option[String] = {
    val t = Option(tableType).getOrElse("").toUpperCase
    if (t.contains("SYSTEM")) None
    else if (t.contains("VIEW")) Some("VIEW")
    else if (t.contains("TABLE")) Some("TABLE")
    else None
  }

  override def tables(schema: String): List[CatalogTable] = withConnection { conn =>
    // schema is a LIKE pattern in JDBC metadata calls: keep exact matches only
    collect(conn.getMetaData.getTables(catalogOf(conn), schema, "%", null)) { rs =>
      if (rs.getString("TABLE_SCHEM") != schema) None
      else
        kindOf(rs.getString("TABLE_TYPE")).map { kind =>
          CatalogTable(rs.getString("TABLE_NAME"), kind, nonEmpty(rs.getString("REMARKS")))
        }
    }.sortBy(_.name)
  }

  private def isDecimal(dataType: Int): Boolean =
    dataType == Types.DECIMAL || dataType == Types.NUMERIC

  override def columns(schema: String, table: String): List[CatalogColumn] =
    withConnection { conn =>
      collect(conn.getMetaData.getColumns(catalogOf(conn), schema, table, "%")) { rs =>
        if (rs.getString("TABLE_SCHEM") != schema || rs.getString("TABLE_NAME") != table) None
        else {
          val dataType = rs.getInt("DATA_TYPE")
          val size = Option(rs.getObject("COLUMN_SIZE")).map(_.toString.toInt)
          val digits = Option(rs.getObject("DECIMAL_DIGITS")).map(_.toString.toInt)
          Some(
            rs.getInt("ORDINAL_POSITION") -> CatalogColumn(
              name = rs.getString("COLUMN_NAME"),
              `type` = rs.getString("TYPE_NAME"),
              nullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
              precision = if (isDecimal(dataType)) size else None,
              scale = if (isDecimal(dataType)) digits else None,
              remarks = nonEmpty(rs.getString("REMARKS"))
            )
          )
        }
      }.sortBy(_._1).map(_._2)
    }

  override def preview(schema: String, table: String, limit: Int): CatalogPreview =
    throw new CatalogException(CatalogErrorCode.QueryFailed, "Preview is not implemented yet")
}
