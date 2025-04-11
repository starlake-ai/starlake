package ai.starlake.sql

import ai.starlake.extract.JdbcDbUtils
import ai.starlake.extract.JdbcDbUtils.StarlakeConnectionPool
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

import java.sql.{Connection, Types}

private object StarlakeSnowflakeDialect extends JdbcDialect with SQLConfHelper {
  override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:snowflake:")
  // override def quoteIdentifier(column: String): String = column
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case TimestampType =>
      Some(JdbcType(sys.env.getOrElse("SF_TIMEZONE", "TIMESTAMP"), java.sql.Types.TIMESTAMP))
    case _ => JdbcDbUtils.getCommonJDBCType(dt)
  }
}

private object StarlakeBigQueryDialect extends JdbcDialect with SQLConfHelper {
  override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:bigquery:")
  // override def quoteIdentifier(column: String): String = column
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType   => Some(JdbcType("BOOL", java.sql.Types.BOOLEAN))
    case IntegerType   => Option(JdbcType("INT64", java.sql.Types.INTEGER))
    case LongType      => Option(JdbcType("INT64", java.sql.Types.BIGINT))
    case DoubleType    => Option(JdbcType("FLOAT64", java.sql.Types.DOUBLE))
    case FloatType     => Option(JdbcType("FLOAT64", java.sql.Types.FLOAT))
    case ShortType     => Option(JdbcType("INT64", java.sql.Types.SMALLINT))
    case ByteType      => Option(JdbcType("INT64", java.sql.Types.TINYINT))
    case StringType    => Option(JdbcType("STRING", java.sql.Types.CLOB))
    case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
    // This is a common case of timestamp without time zone. Most of the databases either only
    // support TIMESTAMP type or use TIMESTAMP as an alias for TIMESTAMP WITHOUT TIME ZONE.
    // Note that some dialects override this setting, e.g. as SQL Server.
    case TimestampNTZType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
    case DateType         => Option(JdbcType("DATE", java.sql.Types.DATE))
    case t: DecimalType =>
      Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
    case _ => JdbcDbUtils.getCommonJDBCType(dt)
  }
}

private object StarlakeDuckDbDialect extends JdbcDialect with SQLConfHelper {

  override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    (partitionId: Int) =>
      {
        try {
          StarlakeConnectionPool.getConnection(options.parameters)
        } catch {
          case e: Throwable =>
            throw new Exception(
              s"Error while creating connection for partition $partitionId",
              e
            )
        }
      }
  }

  override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:duckdb:")
  // override def quoteIdentifier(column: String): String = column
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case _           => JdbcDbUtils.getCommonJDBCType(dt)
  }
  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): Option[DataType] = {
    if (sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
      Some(TimestampType)
    } else None
  }

}

object StarlakeJdbcDialects {
  val dialects = List(StarlakeSnowflakeDialect, StarlakeDuckDbDialect, StarlakeBigQueryDialect)
  def registerDialects() =
    dialects.foreach { dialect =>
      JdbcDialects.registerDialect(dialect)
    }
}
