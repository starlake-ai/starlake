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

import ai.starlake.extract.JdbcDbUtils.SqlColumn
import ai.starlake.schema.handlers.SchemaHandler
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.*

import java.sql.{Date, Timestamp}
import java.text.{DecimalFormat, NumberFormat, SimpleDateFormat}
import java.time.*
import java.time.format.DateTimeFormatter.*
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
  def fromString(str: String, pattern: String, zone: Option[String] = None): Any

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
      case "variant"   => PrimitiveType.variant
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
    def fromString(str: String, pattern: String, zone: Option[String]): Any = str

    def sparkType(zone: Option[String]): DataType = StringType
  }

  object variant extends PrimitiveType("variant") {
    def fromString(str: String, pattern: String, zone: Option[String]): Any = str

    def sparkType(zone: Option[String]): DataType = StringType // VarcharType(Int.MaxValue)
  }

  object long extends PrimitiveType("long") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.trim.toLong

    def sparkType(zone: Option[String]): DataType = LongType
  }

  object int extends PrimitiveType("int") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.trim.toInt

    def sparkType(zone: Option[String]): DataType = IntegerType
  }

  object short extends PrimitiveType("short") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.trim.toShort

    def sparkType(zone: Option[String]): DataType = ShortType
  }

  object double extends PrimitiveType("double") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        typedConvert(str, zone)
      }
    }

    private def typedConvert(str: String, zone: Option[String]) = {
      zone match {
        case Some(zone) =>
          val locale = zone.split('_')
          val currentLocale: Locale = new Locale(locale(0), locale(1))
          val numberFormatter =
            NumberFormat.getNumberInstance(currentLocale).asInstanceOf[DecimalFormat]
          if (str.head == '+')
            numberFormatter.setPositivePrefix("+")
          numberFormatter.parse(str.trim).doubleValue()
        case None => str.trim.toDouble
      }
    }

    def sparkType(zone: Option[String]): DataType = DoubleType

    def parseUDF(zone: Option[String]) = {
      udf((input: String) => {
        Option(input).flatMap(str => {
          Try(typedConvert(str, zone)).toOption
        })
      }).withName("as_double")
    }
  }

  object decimal extends PrimitiveType("decimal") {
    val defaultDecimalType = DataTypes.createDecimalType(38, 9)
    var decimals: mutable.Map[String, DecimalType] = mutable.Map.empty
    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else BigDecimal(str.trim)

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

    private def getPatterns(pattern: String) = {
      if (pattern.indexOf("<-TF->") >= 0) {
        val tf = pattern.split("<-TF->")
        val (truePattern, falsePattern) = (
          Pattern
            .compile(tf(0), Pattern.DOTALL),
          Pattern
            .compile(tf(1), Pattern.DOTALL)
        )
        truePattern -> falsePattern
      } else {
        throw new Exception(
          s"Boolean pattern must have '<-TF->' in its pattern $pattern to validate input and choose between true or false"
        )
      }
    }

    private def resolveBoolean(
      truePattern: Pattern,
      falsePattern: Pattern,
      value: String,
      trim: Boolean
    ): Option[Boolean] = {
      val str = if (trim) {
        value.trim
      } else {
        value
      }
      if (truePattern.matcher(str.trim).matches())
        Some(true)
      else if (falsePattern.matcher(str.trim).matches())
        Some(false)
      else
        None
    }

    def fromString(str: String, pattern: String, zone: Option[String]) = {
      if (str == null || str.isEmpty)
        null
      else {
        val (truePattern, falsePattern) = getPatterns(pattern)
        resolveBoolean(truePattern, falsePattern, str, trim = true).getOrElse(
          throw new Exception(s"value $str does not match $pattern")
        )
      }
    }

    def sparkType(zone: Option[String]): DataType = BooleanType

    def parseUDF(pattern: String) = {
      val (truePattern, falsePattern) = getPatterns(pattern)
      udf((str: String) => {
        Option(str).flatMap { str =>
          resolveBoolean(truePattern, falsePattern, str, trim = false)
        }
      }).withName("as_boolean")
    }
  }

  object byte extends PrimitiveType("byte") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.head.toByte

    def sparkType(zone: Option[String]): DataType = ByteType

    // Could not find the equivalent spark sql expression
    def parseUDF =
      udf((str: String) => Option(str).flatMap(_.headOption).flatMap(c => Try(c.toByte).toOption))
        .withName("as_byte")
  }

  object struct extends PrimitiveType("struct") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.toByte

    def sparkType(zone: Option[String]): DataType = new StructType(Array.empty[StructField])
  }

  private def instantFromString(str: String, pattern: String, zoneId: ZoneId): Instant = {

    def simpleDateFormat(str: String, pattern: String, zoneId: ZoneId): Instant = {
      if (zoneId.getId != "UTC")
        throw new IllegalArgumentException(
          s"Explicit zoneId $zoneId not supported for pattern $pattern"
        )
      val df = new SimpleDateFormat(pattern)
      df.setTimeZone(TimeZone.getTimeZone("UTC"))
      val date = df.parse(str.trim)
      Instant.ofEpochMilli(date.getTime)
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

    override def fromString(str: String, pattern: String, zone: Option[String]): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        typedConvert(str, pattern, zone)
      }
    }

    private def typedConvert(str: String, pattern: String, zone: Option[String]): Date = {
      val formatter = zone match {
        case None => DateTimeFormatter.ofPattern(pattern)
        case Some(zone) =>
          val locale = zone.split('_')
          val currentLocale: Locale = new Locale(locale(0), locale(1))
          new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern(pattern)
            .toFormatter
            .withLocale(currentLocale)
      }
      Try {
        val date = LocalDate.parse(str.trim, formatter)
        java.sql.Date.valueOf(date)
      }.getOrElse(
        java.sql.Date.valueOf(YearMonth.parse(str.trim, formatter).atDay(1))
      )
    }

    def sparkType(zone: Option[String]): DataType = DateType

    def parseUDF(pattern: String, zone: Option[String]) =
      udf((str: String) =>
        Option(str).flatMap { str =>
          Try(typedConvert(str, pattern, zone)).toOption
        }
      ).withName("as_date")
  }

  object timestamp extends PrimitiveType("timestamp") {

    def fromString(str: String, timeFormat: String, zone: Option[String]): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        typedConvert(str, timeFormat, zone)
      }
    }

    private def typedConvert(str: String, timeFormat: String, zone: Option[String]): Timestamp = {
      val zoneId = zone match {
        case Some(z) => ZoneId.of(z)
        case None    => ZoneId.of("UTC")
      }

      val instant = instantFromString(str.trim, timeFormat, zoneId)
      val tsValue = Timestamp.from(instant)
      tsValue
    }

    def sparkType(zone: Option[String]): DataType = TimestampType

    def parseUDF(timeFormat: String, zone: Option[String]) =
      udf((str: String) =>
        Option(str).flatMap { str =>
          Try(typedConvert(str, timeFormat, zone)).toOption
        }
      ).withName("as_timestamp")
  }

  val primitiveTypes: Set[PrimitiveType] =
    Set(string, long, int, double, short, decimal, boolean, byte, date, timestamp, struct, variant)

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

  // fail to compile here because we need to get the type from the ddlMapping
  def fromSQLType(db: String, sqlCol: SqlColumn, schemaHandler: SchemaHandler): PrimitiveType = {
    val typesInfo = TypesInfo(1, schemaHandler.types())
    typesInfo.getApproximateTypeForSqlType(db, sqlCol.sqlTypeAsString()) match {
      case Some(typeInfo) => typeInfo.primitiveType
      case None =>
        val sqlTypePrefix = sqlCol.dataType.takeWhile(it => it.isLetter)
        sqlTypePrefix.toLowerCase match {
          case "varchar" | "char" | "text" | "string" | "uuid" => PrimitiveType.string
          case "int" | "integer"                               => PrimitiveType.int
          case "smallint"                                      => PrimitiveType.int
          case "bigint" | "number"                             => PrimitiveType.long
          case "float" | "double" | "real"                     => PrimitiveType.double
          case "decimal" | "numeric"                           => PrimitiveType.decimal
          case "boolean" | "bool"                              => PrimitiveType.boolean
          case "tinyint"                                       => PrimitiveType.int
          case "date"                                          => PrimitiveType.date
          case "timestamp" | "datetime"                        => PrimitiveType.timestamp
          case _                                               => PrimitiveType.string
        }
    }
  }

  def from(elementType: DataType): PrimitiveType = {
    elementType match {
      case _: BinaryType    => PrimitiveType.string
      case _: ByteType      => PrimitiveType.byte
      case _: ShortType     => PrimitiveType.short
      case _: IntegerType   => PrimitiveType.int
      case _: LongType      => PrimitiveType.long
      case _: BooleanType   => PrimitiveType.boolean
      case _: FloatType     => PrimitiveType.double
      case _: DoubleType    => PrimitiveType.double
      case _: DecimalType   => PrimitiveType.decimal
      case _: StringType    => PrimitiveType.string
      case _: TimestampType => PrimitiveType.timestamp
      case _: DateType      => PrimitiveType.date
      case _: VarcharType   => PrimitiveType.variant
      case _ =>
        throw new IllegalArgumentException("Data type not expected: " + elementType.simpleString)
    }
  }

}
