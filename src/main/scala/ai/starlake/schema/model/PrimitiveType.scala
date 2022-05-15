/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.text.{DecimalFormat, NumberFormat, SimpleDateFormat}
import java.time._
import java.time.format.DateTimeFormatter._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.TemporalAccessor
import java.util.regex.Pattern
import java.util.{Locale, TimeZone}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Spark supported primitive types. These are the only valid raw types. Dataframes columns are
  * converted to these types before the dataset is ingested
  *
  * @param value
  *   : string, long, double, boolean, byte, date, timestamp, decimal with (precision=30, scale=15)
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[PrimitiveTypeDeserializer])
sealed abstract case class PrimitiveType(value: String) {
  def fromString(str: String, pattern: String = null, zone: String = null): Any

  override def toString: String = value

  def sparkType(zone: Option[String]): DataType
}

class PrimitiveTypeDeserializer extends JsonDeserializer[PrimitiveType] {

  def simpleTypeFromString(value: String): PrimitiveType = {
    value match {
      case "string"    => PrimitiveType.string
      case "long"      => PrimitiveType.long
      case "int"       => PrimitiveType.int
      case "short"     => PrimitiveType.short
      case "double"    => PrimitiveType.double
      case "boolean"   => PrimitiveType.boolean
      case "byte"      => PrimitiveType.byte
      case "date"      => PrimitiveType.date
      case "timestamp" => PrimitiveType.timestamp
      case "decimal"   => PrimitiveType.decimal
      case "struct"    => PrimitiveType.struct
      case _ =>
        throw new Exception(
          s"Invalid primitive type: $value not in ${PrimitiveType.primitiveTypes}"
        )
    }
  }

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrimitiveType = {
    val value = jp.readValueAs[String](classOf[String])
    simpleTypeFromString(value)
  }
}

object PrimitiveType {

  object string extends PrimitiveType("string") {
    def fromString(str: String, pattern: String, zone: String): Any = str

    def sparkType(zone: Option[String]): DataType = StringType
  }

  object long extends PrimitiveType("long") {

    def fromString(str: String, pattern: String, zone: String): Any =
      if (str == null || str.isEmpty) null else str.toLong

    def sparkType(zone: Option[String]): DataType = LongType
  }

  object int extends PrimitiveType("int") {

    def fromString(str: String, pattern: String, zone: String): Any =
      if (str == null || str.isEmpty) null else str.toInt

    def sparkType(zone: Option[String]): DataType = IntegerType
  }

  object short extends PrimitiveType("short") {

    def fromString(str: String, pattern: String, zone: String): Any =
      if (str == null || str.isEmpty) null else str.toShort

    def sparkType(zone: Option[String]): DataType = ShortType
  }

  object double extends PrimitiveType("double") {

    def fromString(str: String, pattern: String, zone: String): Any = {
      if (str == null || str.isEmpty)
        null
      else if (zone == null)
        str.toDouble
      else {
        val locale = zone.split('_')
        val currentLocale: Locale = new Locale(locale(0), locale(1))
        val numberFormatter =
          NumberFormat.getNumberInstance(currentLocale).asInstanceOf[DecimalFormat]
        if (str.head == '+')
          numberFormatter.setPositivePrefix("+")
        numberFormatter.parse(str).doubleValue()
      }
    }

    def sparkType(zone: Option[String]): DataType = DoubleType
  }

  object decimal extends PrimitiveType("decimal") {
    val defaultDecimalType = DataTypes.createDecimalType(38, 9)
    var decimals: mutable.Map[String, DecimalType] = mutable.Map.empty
    def fromString(str: String, pattern: String, zone: String): Any =
      if (str == null || str.isEmpty) null else BigDecimal(str)

    override def sparkType(zone: Option[String]): DataType = {
      zone match {
        case None =>
          defaultDecimalType
        case Some(zone) =>
          Try {
            val precisionScale = zone.split(",")
            val precision = precisionScale(0).toInt
            val scale = precisionScale(1).toInt
            val key = s"$precision/$scale"
            decimals.get(key) match {
              case None =>
                val newDecimalType = DataTypes.createDecimalType(precision, scale)
                decimals.put(key, newDecimalType)
                newDecimalType
              case Some(decimalType) => decimalType
            }
          } match {
            case Success(res) => res
            case Failure(_)   => defaultDecimalType
          }
      }
    }
  }

  object boolean extends PrimitiveType("boolean") {

    def matches(str: String, truePattern: Pattern, falsePattern: Pattern): Boolean = {
      truePattern
        .matcher(str)
        .matches() ||
      falsePattern
        .matcher(str)
        .matches()
    }

    def fromString(str: String, pattern: String, zone: String): Any = {
      if (pattern.indexOf("<-TF->") >= 0) {
        val tf = pattern.split("<-TF->")
        if (Pattern.compile(tf(0), Pattern.MULTILINE).matcher(str).matches())
          true
        else if (Pattern.compile(tf(1), Pattern.MULTILINE).matcher(str).matches())
          false
        else
          throw new Exception(s"value $str does not match $pattern")
      } else {
        throw new Exception(s"Operator <-TF-> required in pattern $pattern to validate $str")
      }
    }

    def sparkType(zone: Option[String]): DataType = BooleanType
  }

  object byte extends PrimitiveType("byte") {

    def fromString(str: String, pattern: String, zone: String): Any =
      if (str == null || str.isEmpty) null else str.head.toByte

    def sparkType(zone: Option[String]): DataType = ByteType
  }

  object struct extends PrimitiveType("struct") {

    def fromString(str: String, pattern: String, zone: String): Any =
      if (str == null || str.isEmpty) null else str.toByte

    def sparkType(zone: Option[String]): DataType = new StructType(Array.empty[StructField])
  }

  private def instantFromString(str: String, pattern: String, zone: String): Instant = {

    def simpleDateFormat(str: String, pattern: String, zoneId: ZoneId): Instant = {
      if (zoneId.getId != "UTC")
        throw new IllegalArgumentException(
          s"Explicit zoneId $zoneId not supported for pattern $pattern"
        )
      val df = new SimpleDateFormat(pattern)
      df.setTimeZone(TimeZone.getTimeZone("UTC"))
      val date = df.parse(str)
      Instant.ofEpochMilli(date.getTime)
    }

    val zoneId = Option(zone) match {
      case Some(z) => ZoneId.of(z)
      case None    => ZoneId.of("UTC")
    }

    pattern match {
      case "epoch_second" =>
        Instant.ofEpochSecond(str.toLong)
      case "epoch_milli" =>
        Instant.ofEpochMilli(str.toLong)
      case _ =>
        val formatter = PrimitiveType.dateFormatters
          .getOrElse(pattern, DateTimeFormatter.ofPattern(pattern))
        Try {
          formatter.parse(str)
        } match {
          case Success(dateTime: TemporalAccessor) =>
            Try(Instant.from(dateTime)) match {
              case Success(instant) =>
                Try {
                  ZoneId.from(dateTime)
                } match {
                  case Success(zone) =>
                    val dateTimeRawOffset = TimeZone.getTimeZone(zone.normalized).getRawOffset
                    val zoneIdRawOffset = TimeZone.getTimeZone(zoneId.normalized).getRawOffset

                    if (zoneId.getId == "UTC" || dateTimeRawOffset == zoneIdRawOffset)
                      instant
                    else
                      throw new IllegalArgumentException(
                        s"Incompatible timezones found in (pattern zone = $zone, configuration zone = $zoneId)"
                      )
                  case Failure(_) =>
                    if (zoneId.getId == "UTC")
                      instant
                    else
                      throw new IllegalArgumentException(
                        s"Incompatible timezones found in (pattern zone = UTC, configuration zone = $zoneId)"
                      )
                }

              case Failure(_) =>
                // Try to parse it as a date without time and still make it a timestamp.
                Try {
                  val localDateTime = LocalDateTime.from(dateTime)
                  ZonedDateTime.of(localDateTime, zoneId).toInstant
                } match {
                  case Success(instant) => instant
                  case Failure(_) =>
                    Try(LocalDate.from(dateTime)) match {
                      case Success(date) =>
                        val zone = Try {
                          ZoneId.from(dateTime)
                        } match {
                          case Success(z)
                              if zoneId.getId == "UTC" | z.normalized.getId == zoneId.normalized.getId =>
                            z
                          case Success(z) =>
                            throw new IllegalArgumentException(
                              s"Incompatible timezones found in (pattern zone = $z, configuration zone = $zoneId)"
                            )
                          case Failure(_) => zoneId
                        }
                        date.atStartOfDay(zone).toInstant
                      case Failure(_) => simpleDateFormat(str, pattern, zoneId)
                    }
                }
            }
          case Failure(_) =>
            // Try to parse it as a date without time and still make it a timestamp.
            // Cloudera 5.X with Hive 1.1 workaround
            // Compatible with time without date
            simpleDateFormat(str, pattern, zoneId)
        }
    }
  }

  object date extends PrimitiveType("date") {

    def fromString(str: String, pattern: String, zone: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        val formatter = Option(zone) match {
          case None => DateTimeFormatter.ofPattern(pattern)
          case Some(zone) =>
            val locale = zone.split('_')
            val currentLocale: Locale = new Locale(locale(0), locale(1))
            new DateTimeFormatterBuilder
              .parseCaseInsensitive()
              .appendPattern(pattern)
              .toFormatter
              .withLocale(currentLocale)
        }
        Try {
          val date = LocalDate.parse(str, formatter)
          java.sql.Date.valueOf(date)
        } match {
          case Success(value) => value
          case Failure(_)     => java.sql.Date.valueOf(YearMonth.parse(str, formatter).atDay(1))
        }
      }
    }
    def sparkType(zone: Option[String]): DataType = DateType
  }

  object timestamp extends PrimitiveType("timestamp") {

    def fromString(str: String, timeFormat: String, zone: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        val instant = instantFromString(str, timeFormat, zone)
        Timestamp.from(instant)
      }
    }

    def sparkType(zone: Option[String]): DataType = TimestampType
  }

  val primitiveTypes: Set[PrimitiveType] =
    Set(string, long, int, double, decimal, boolean, byte, date, timestamp, struct)

  val dateFormatters: Map[String, DateTimeFormatter] = Map(
    // ISO_LOCAL_TIME, ISO_OFFSET_TIME and ISO_TIME patterns are specific to time handling, without a date
    // We choose not to handle them
    "BASIC_ISO_DATE"       -> BASIC_ISO_DATE,
    "ISO_LOCAL_DATE"       -> ISO_LOCAL_DATE,
    "ISO_OFFSET_DATE"      -> ISO_OFFSET_DATE,
    "ISO_DATE"             -> ISO_DATE,
    "ISO_LOCAL_DATE_TIME"  -> ISO_LOCAL_DATE_TIME,
    "ISO_OFFSET_DATE_TIME" -> ISO_OFFSET_DATE_TIME,
    "ISO_ZONED_DATE_TIME"  -> ISO_ZONED_DATE_TIME,
    "ISO_DATE_TIME"        -> ISO_DATE_TIME,
    "ISO_ORDINAL_DATE"     -> ISO_ORDINAL_DATE,
    "ISO_WEEK_DATE"        -> ISO_WEEK_DATE,
    "ISO_INSTANT"          -> ISO_INSTANT,
    "RFC_1123_DATE_TIME"   -> RFC_1123_DATE_TIME
  )

}
