package com.ebiznext.comet.schema.model

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}


/**
  * Spark supported primitive types. These are the only valid raw types.
  * Dataframes columns are converted to these types before the data is ingested
  *
  * @param value : string, long, double, boolean, byte, date, timestamp, decimal with (precision=30, scale=15)
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[PrimitiveTypeDeserializer])
sealed abstract case class PrimitiveType(value: String) {
  def fromString(str: String, dateFormat: String = null): Any

  override def toString: String = value

  def sparkType: DataType
}


class PrimitiveTypeDeserializer extends JsonDeserializer[PrimitiveType] {
  def simpleTypeFromString(value: String): PrimitiveType = {
    value match {
      case "string" => PrimitiveType.string
      case "long" => PrimitiveType.long
      case "double" => PrimitiveType.double
      case "boolean" => PrimitiveType.boolean
      case "byte" => PrimitiveType.byte
      case "date" => PrimitiveType.date
      case "timestamp" => PrimitiveType.timestamp
      case "decimal" => PrimitiveType.decimal
      case "struct" => PrimitiveType.struct
      case _ => throw new Exception(s"Invalid primitive type: $value not in ${PrimitiveType.primitiveTypes}")
    }
  }

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrimitiveType = {
    val value = jp.readValueAs[String](classOf[String])
    simpleTypeFromString(value)
  }
}

object PrimitiveType {

  object string extends PrimitiveType("string") {
    def fromString(str: String, dateFormat: String = null): Any = str

    def sparkType: DataType = StringType
  }

  object long extends PrimitiveType("long") {
    def fromString(str: String, dateFormat: String): Any = if (str == null || str.isEmpty) null else str.toLong

    def sparkType: DataType = LongType
  }

  object double extends PrimitiveType("double") {
    def fromString(str: String, dateFormat: String): Any = if (str == null || str.isEmpty) null else str.toDouble

    def sparkType: DataType = DoubleType
  }

  object decimal extends PrimitiveType("decimal") {
    val defaultDecimalType = DataTypes.createDecimalType(30, 15)

    def fromString(str: String, dateFormat: String): Any = if (str == null || str.isEmpty) null else BigDecimal(str)

    override def sparkType: DataType = defaultDecimalType
  }

  object boolean extends PrimitiveType("boolean") {
    def fromString(str: String, dateFormat: String): Any = if (str == null || str.isEmpty) null else str.toBoolean

    def sparkType: DataType = BooleanType
  }

  object byte extends PrimitiveType("byte") {
    def fromString(str: String, dateFormat: String): Any = if (str == null || str.isEmpty) null else str.toByte

    def sparkType: DataType = ByteType
  }

  object struct extends PrimitiveType("struct") {
    def fromString(str: String, dateFormat: String): Any = if (str == null || str.isEmpty) null else str.toByte

    def sparkType: DataType = new StructType(Array.empty[StructField])
  }

  private def instantFromString(str: String, format: String): Instant = {
    import java.time.format.DateTimeFormatter
    format match {
      case "epoch_second" =>
        Instant.ofEpochSecond(str.toLong)
      case "epoch_milli" =>
        Instant.ofEpochMilli(str.toLong)
      case _ =>
        val formatter = PrimitiveType.formatters.getOrElse(format, DateTimeFormatter.ofPattern(format))
        val dateTime: TemporalAccessor = formatter.parse(str)
        Try(Instant.from(dateTime)) match {
          case Success(instant) =>
            instant

          case Failure(_) =>
//            val localDateTime = LocalDateTime.from(dateTime)
//            ZonedDateTime.of(localDateTime, ZoneId.systemDefault()).toInstant
            Try {
              val localDateTime = LocalDateTime.from(dateTime)
              ZonedDateTime.of(localDateTime, ZoneId.systemDefault()).toInstant
            } match {
              case Success(instant) =>
                instant
              case Failure(_) =>
                // Try to parse it as a date without time and still make it a timestamp.
                // Cloudera 5.X with Hive 1.1 workaround
                import java.text.SimpleDateFormat
                val df = new SimpleDateFormat(format)
                val date = df.parse(str)
                Instant.ofEpochMilli(date.getTime)
            }
        }
    }
  }


  object date extends PrimitiveType("date") {
    def fromString(str: String, dateFormat: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        import java.text.SimpleDateFormat
        val df = new SimpleDateFormat(dateFormat)
        val date = df.parse(str)
        new java.sql.Date(date.getTime)

      }
    }

    def sparkType: DataType = DateType
  }

  object timestamp extends PrimitiveType("timestamp") {
    def fromString(str: String, timeFormat: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        val instant = instantFromString(str, timeFormat)
        Timestamp.from(instant)
      }
    }

    def sparkType: DataType = TimestampType
  }

  val primitiveTypes: Set[PrimitiveType] = Set(string, long, double, decimal, boolean, byte, date, timestamp, struct)

  import DateTimeFormatter._

  val formatters = Map(
    "BASIC_ISO_DATE" -> BASIC_ISO_DATE,
    "ISO_LOCAL_DATE" -> ISO_LOCAL_DATE,
    "ISO_OFFSET_DATE" -> ISO_OFFSET_DATE,
    "ISO_DATE" -> ISO_DATE,
    "ISO_LOCAL_TIME" -> ISO_LOCAL_TIME,
    "ISO_OFFSET_TIME" -> ISO_OFFSET_TIME,
    "ISO_TIME" -> ISO_TIME,
    "ISO_LOCAL_DATE_TIME" -> ISO_LOCAL_DATE_TIME,
    "ISO_OFFSET_DATE_TIME" -> ISO_OFFSET_DATE_TIME,
    "ISO_ZONED_DATE_TIME" -> ISO_ZONED_DATE_TIME,
    "ISO_DATE_TIME" -> ISO_DATE_TIME,
    "ISO_ORDINAL_DATE" -> ISO_ORDINAL_DATE,
    "ISO_WEEK_DATE" -> ISO_WEEK_DATE,
    "ISO_INSTANT" -> ISO_INSTANT,
    "RFC_1123_DATE_TIME" -> RFC_1123_DATE_TIME)


}
