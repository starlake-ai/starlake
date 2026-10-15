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

  private def quoteIdentifier(identifier: String, quote: String): String =
    if (quote == null || quote.trim.isEmpty) identifier
    else quote + identifier.replace(quote, quote + quote) + quote

  private[catalog] def jsonValue(value: Any): Any = value match {
    case null                 => null
    case b: java.lang.Boolean => b
    case n @ (_: java.lang.Integer | _: java.lang.Long | _: java.lang.Short | _: java.lang.Byte) =>
      CatalogJson.safeInteger(n.asInstanceOf[Number].longValue())
    case b: java.math.BigInteger =>
      if (b.bitLength < 64) CatalogJson.safeInteger(b.longValue()) else b.toString
    case d: java.lang.Double if d.isNaN || d.isInfinite => d.toString
    case f: java.lang.Float if f.isNaN || f.isInfinite  => f.toString
    case n @ (_: java.lang.Double | _: java.lang.Float) => n.asInstanceOf[Number].doubleValue()
    case d: java.math.BigDecimal                        => d.toPlainString
    case bytes: Array[Byte]                             => s"<binary ${bytes.length} bytes>"
    case other                                          => other.toString
  }

  override def preview(schema: String, table: String, limit: Int): CatalogPreview =
    withConnection { conn =>
      val quote = conn.getMetaData.getIdentifierQuoteString
      val sql =
        s"SELECT * FROM ${quoteIdentifier(schema, quote)}.${quoteIdentifier(table, quote)}"
      Using.resource(conn.createStatement()) { st =>
        st.setMaxRows(limit + 1)
        Using.resource(st.executeQuery(sql)) { rs =>
          val meta = rs.getMetaData
          val columns = (1 to meta.getColumnCount).toList.map { i =>
            CatalogPreviewColumn(meta.getColumnLabel(i), meta.getColumnTypeName(i))
          }
          val rows = ListBuffer[List[Any]]()
          while (rows.size <= limit && rs.next())
            rows += columns.indices.toList.map(i => jsonValue(rs.getObject(i + 1)))
          CatalogPreview(columns, rows.take(limit).toList, truncated = rows.size > limit)
        }
      }
    }
}
