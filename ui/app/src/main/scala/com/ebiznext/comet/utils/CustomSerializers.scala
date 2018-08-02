package com.ebiznext.comet.utils
import java.sql.Timestamp
import java.time.{ LocalDate, LocalDateTime }
import java.time.format.DateTimeFormatter

import org.json4s.CustomSerializer
import org.json4s.JsonAST.{ JInt, JNull, JString }

import scala.util.Try

/**
 * Created by Mourad on 24/07/2018.
 */
object CustomSerializers {

  val all = List(TimestampSerializer, LocalDateTimeISOSerializer, LocalDateISOSerializer)

}

case object TimestampSerializer
  extends CustomSerializer[Timestamp](format =>
    ({
      case JInt(x) => new Timestamp(x.longValue * 1000)
      case JNull => null
    }, {
      case date: Timestamp => JInt(date.getTime / 1000)
    }))

case object LocalDateTimeISOSerializer
  extends CustomSerializer[LocalDateTime](format => {

    val isoFormat = DateTimeFormatter.ISO_DATE_TIME
    def isValidDateTime(str: String): Boolean = Try(isoFormat.parse(str)).isSuccess

    (
      {
        case JString(value) if isValidDateTime(value) =>
          LocalDateTime.parse(value, isoFormat)
      }, {
        case ldt: LocalDateTime => JString(ldt.format(isoFormat))
      }
    )
  })

case object LocalDateISOSerializer
  extends CustomSerializer[LocalDate](format => {

    val isoFormat = DateTimeFormatter.ISO_DATE
    def isValidDate(str: String): Boolean = Try(isoFormat.parse(str)).isSuccess

    (
      {
        case JString(value) if isValidDate(value) =>
          LocalDate.parse(value, isoFormat)
      }, {
        case ldt: LocalDate => JString(ldt.format(isoFormat))
      }
    )
  })
