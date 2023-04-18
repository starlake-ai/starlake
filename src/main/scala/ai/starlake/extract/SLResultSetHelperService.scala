package ai.starlake.extract

/** An adapted code of au.com.bytecode.opencsv.ResultSetHelperService since we can't customize value
  * by type serialization.
  */

import au.com.bytecode.opencsv.ResultSetHelper

import java.io.BufferedReader
import java.sql.{Array => _, _}
import java.text.SimpleDateFormat
import java.util

object SLResultSetHelperService {
  private def read(c: Clob): String = {
    val sb = new StringBuilder(c.length.toInt)
    val br = new BufferedReader(c.getCharacterStream())

    def appendToSB(): Unit = {
      Option(br.readLine()) match {
        case Some(line) =>
          sb.append(line)
          appendToSB()
        case None => // end of reading line
      }
    }
    appendToSB()
    sb.toString
  }
}
class SLResultSetHelperService(
  private val datePattern: String,
  private val timestampPattern: String
) extends ResultSetHelper {
  override def getColumnNames(rs: ResultSet): Array[String] = {
    val names = new util.ArrayList[String]
    val metadata = rs.getMetaData
    for (i <- 0 until metadata.getColumnCount) {
      names.add(metadata.getColumnName(i + 1))
    }
    val nameArray = new Array[String](names.size)
    names.toArray(nameArray)
  }

  override def getColumnValues(rs: ResultSet): Array[String] = {
    val values = new util.ArrayList[String]
    val metadata = rs.getMetaData
    for (i <- 0 until metadata.getColumnCount) {
      values.add(getColumnValue(rs, metadata.getColumnType(i + 1), i + 1))
    }
    val valueArray = new Array[String](values.size)
    values.toArray(valueArray)
  }

  private def getAsStringOrNull[T](resultSet: ResultSet, dataGetter: ResultSet => T): String =
    getAndTransformAsStringOrNull(resultSet, dataGetter)(identity)

  private def getAndTransformAsStringOrNull[T, U](resultSet: ResultSet, dataGetter: ResultSet => T)(
    transform: T => U
  ): String =
    Option(dataGetter(resultSet))
      .filterNot(_ => resultSet.wasNull())
      .map(transform)
      .map(_.toString)
      .orNull

  private def handleDate(date: Date) = {
    val dateFormat = new SimpleDateFormat(datePattern)
    dateFormat.format(date)
  }

  private def handleTimestamp(timestamp: Timestamp): String = {
    val timeFormat = new SimpleDateFormat(timestampPattern)
    timeFormat.format(timestamp)
  }

  private def getColumnValue(rs: ResultSet, colType: Int, colIndex: Int): String = {
    colType match {
      case Types.BIT | Types.JAVA_OBJECT =>
        getAsStringOrNull(rs, _.getObject(colIndex))

      case Types.BOOLEAN =>
        getAsStringOrNull(rs, _.getBoolean(colIndex))

      case Types.NCLOB =>
        getAndTransformAsStringOrNull(rs, _.getNClob(colIndex))(SLResultSetHelperService.read)
      case Types.CLOB =>
        getAndTransformAsStringOrNull(rs, _.getClob(colIndex))(SLResultSetHelperService.read)
      case Types.BIGINT =>
        getAsStringOrNull(rs, _.getLong(colIndex))

      case Types.DECIMAL | Types.DOUBLE | Types.FLOAT | Types.REAL | Types.NUMERIC =>
        getAsStringOrNull(rs, _.getBigDecimal(colIndex))

      case Types.INTEGER | Types.TINYINT | Types.SMALLINT =>
        getAsStringOrNull(rs, _.getInt(colIndex))

      case Types.DATE =>
        getAndTransformAsStringOrNull(rs, _.getDate(colIndex))(handleDate)

      case Types.TIME | Types.TIME_WITH_TIMEZONE =>
        getAsStringOrNull(rs, _.getTime(colIndex))

      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        getAndTransformAsStringOrNull(rs, _.getTimestamp(colIndex))(handleTimestamp)

      case Types.NVARCHAR | Types.NCHAR | Types.LONGNVARCHAR =>
        rs.getNString(colIndex)

      case Types.LONGVARCHAR | Types.VARCHAR | Types.CHAR =>
        rs.getString(colIndex)
      case _ =>
        getAsStringOrNull(rs, _.getObject(colIndex))
    }
  }
}
