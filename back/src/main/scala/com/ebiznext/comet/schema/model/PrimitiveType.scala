package com.ebiznext.comet.schema.model

import java.sql.Timestamp
import java.time.temporal.TemporalAccessor
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

import scala.util.{Failure, Success, Try}


/**
  * Spark supported primitive types. These are the only valid raw types.
  * Dataframes columns are converted to these types before the data is ingested
  *
  * @param value : string, long, double, boolean, byte, date, timestamp
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[PrimitiveTypeDeserializer])
sealed abstract case class PrimitiveType(value: String) {
  def fromString(str: String, dateFormat: String = null, timeFormat: String = null): Any

  override def toString: String = value
}


class PrimitiveTypeDeserializer extends JsonDeserializer[PrimitiveType] {
  def simpleTypeFromString(value: String): Option[PrimitiveType] = {
    value match {
      case "string" => Some(PrimitiveType.string)
      case "long" => Some(PrimitiveType.long)
      case "double" => Some(PrimitiveType.double)
      case "boolean" => Some(PrimitiveType.boolean)
      case "byte" => Some(PrimitiveType.byte)
      case "date" => Some(PrimitiveType.date)
      case "timestamp" => Some(PrimitiveType.timestamp)
      case _ => None
    }
  }

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrimitiveType = {
    val value = jp.readValueAs[String](classOf[String])
    simpleTypeFromString(value) match {
      case Some(tpe) => tpe
      case None =>
        value match {
          case "map" => PrimitiveType.map
          case "array" => PrimitiveType.array
          case tpe if tpe.startsWith("array_of_") =>
            val subtypeString = tpe.substring("array_of_".length)
            val subtpe = simpleTypeFromString(subtypeString).getOrElse(throw new Exception)
            subtpe
          case tpe =>
            throw new Exception(s"Invalid Primitive Type $tpe")
        }
    }
  }
}

object PrimitiveType {

  object string extends PrimitiveType("string") {
    def fromString(str: String, dateFormat: String = null, timeFormat: String = null): Any = str
  }

  object long extends PrimitiveType("long") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = if (str == null || str.isEmpty) null else str.toLong
  }

  object double extends PrimitiveType("double") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = if (str == null || str.isEmpty) null else str.toDouble
  }

  object boolean extends PrimitiveType("boolean") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = if (str == null || str.isEmpty) null else str.toBoolean
  }

  object byte extends PrimitiveType("byte") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = if (str == null || str.isEmpty) null else str.toByte
  }

  object array extends PrimitiveType("array") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = throw new Exception
  }

  object map extends PrimitiveType("map") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = throw new Exception
  }

  private def instantFromString(str: String, format: String): Instant = {
    import java.time.format.DateTimeFormatter
    val formatter = DateTimeFormatter.ofPattern(format)
    val dateTime: TemporalAccessor = formatter.parse(str)
    Try(Instant.from(dateTime)) match {
      case Success(instant) =>
        instant

      case Failure(ex) =>
        val localDateTime = LocalDateTime.from(dateTime)
        ZonedDateTime.of(localDateTime, ZoneId.systemDefault()).toInstant
    }
  }

  object date extends PrimitiveType("date") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        import java.text.SimpleDateFormat
        val df = new SimpleDateFormat(dateFormat)
        val date = df.parse(str)
        new java.sql.Date(date.getTime)

      }
    }
  }

  object timestamp extends PrimitiveType("timestamp") {
    def fromString(str: String, dateFormat: String, timeFormat: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        val instant = instantFromString(str, timeFormat)
        Timestamp.from(instant)
      }
    }
  }

  val primitiveTypes: Set[PrimitiveType] = Set(string, long, double, boolean, byte, date, timestamp, array, map)
}
