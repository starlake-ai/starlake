package com.ebiznext.comet.schema.model

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.regex.Pattern

import com.ebiznext.comet.schema.model.SchemaModel.Format.DSV
import com.ebiznext.comet.schema.model.SchemaModel.Mode.FILE
import com.ebiznext.comet.schema.model.SchemaModel.Write.{APPEND, OVERWRITE}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructField

import scala.util.{Failure, Success, Try}

object SchemaModel {

  @JsonSerialize(using = classOf[ToStringSerializer])
  @JsonDeserialize(using = classOf[ModeDeserializer])
  sealed case class Mode(value: String) {
    override def toString: String = value
  }

  object Mode {
    def fromString(value: String): Mode = {
      value.toUpperCase() match {
        case "FILE" => Mode.FILE
        case "STREAM" => Mode.STREAM
      }
    }

    object FILE extends Mode("FILE")

    object STREAM extends Mode("STREAM")

  }

  class ModeDeserializer extends JsonDeserializer[Mode] {
    override def deserialize(jp: JsonParser, ctx: DeserializationContext): Mode = {
      val value = jp.readValueAs[String](classOf[String])
      Mode.fromString(value)
    }
  }

  @JsonSerialize(using = classOf[ToStringSerializer])
  @JsonDeserialize(using = classOf[WriteDeserializer])
  sealed case class Write(value: String) {
    override def toString: String = value

    def toSaveMode: SaveMode = {
      this match {
        case OVERWRITE => SaveMode.Overwrite
        case APPEND => SaveMode.Append
      }
    }
  }

  object Write {
    def fromString(value: String): Write = {
      value.toUpperCase() match {
        case "OVERWRITE" => Write.OVERWRITE
        case "APPEND" => Write.APPEND
      }
    }

    object OVERWRITE extends Write("OVERWRITE")

    object APPEND extends Write("APPEND")

  }

  class WriteDeserializer extends JsonDeserializer[Write] {
    override def deserialize(jp: JsonParser, ctx: DeserializationContext): Write = {
      val value = jp.readValueAs[String](classOf[String])
      Write.fromString(value)
    }
  }

  @JsonSerialize(using = classOf[ToStringSerializer])
  @JsonDeserialize(using = classOf[FormatDeserializer])
  sealed case class Format(value: String) {
    override def toString: String = value
  }

  object Format {
    def fromString(value: String): Format = {
      value.toUpperCase match {
        case "DSV" => Format.DSV
        case "JSON" => Format.JSON
      }
    }

    object DSV extends Format("DSV")

    object JSON extends Format("JSON")

  }

  class FormatDeserializer extends JsonDeserializer[Format] {
    override def deserialize(jp: JsonParser, ctx: DeserializationContext): Format = {
      val value = jp.readValueAs[String](classOf[String])
      Format.fromString(value)
    }
  }

  @JsonSerialize(using = classOf[ToStringSerializer])
  @JsonDeserialize(using = classOf[PrivacyLevelDeserializer])
  sealed case class PrivacyLevel(value: String) {
    override def toString: String = value
  }

  object PrivacyLevel {
    def fromString(value: String): PrivacyLevel = {
      value.toUpperCase() match {
        case "NONE" => PrivacyLevel.NONE
        case "HIDE" => PrivacyLevel.HIDE
        case "MD5" => PrivacyLevel.MD5
        case "SHA1" => PrivacyLevel.SHA1
        case "AES" => PrivacyLevel.AES
      }
    }

    object NONE extends PrivacyLevel("NONE")

    object HIDE extends PrivacyLevel("HIDE")

    object MD5 extends PrivacyLevel("MD5")

    object SHA1 extends PrivacyLevel("SHA1")

    object AES extends PrivacyLevel("AES")

  }

  class PrivacyLevelDeserializer extends JsonDeserializer[PrivacyLevel] {
    override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrivacyLevel = {
      val value = jp.readValueAs[String](classOf[String])
      PrivacyLevel.fromString(value)
    }
  }

  @JsonSerialize(using = classOf[ToStringSerializer])
  @JsonDeserialize(using = classOf[PrimitiveTypeDeserializer])
  sealed abstract case class PrimitiveType(value: String) {
    def fromString(str: String, format: String = null): Any

    override def toString: String = value
  }


  class PrimitiveTypeDeserializer extends JsonDeserializer[PrimitiveType] {
    override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrimitiveType = {
      val value = jp.readValueAs[String](classOf[String])
      value match {
        case "string" => PrimitiveType.string
        case "long" => PrimitiveType.long
        case "double" => PrimitiveType.double
        case "boolean" => PrimitiveType.boolean
        case "byte" => PrimitiveType.byte
        case "date" => PrimitiveType.date
        case "timestamp" => PrimitiveType.timestamp
      }
    }
  }

  object PrimitiveType {

    object string extends PrimitiveType("string") {
      def fromString(str: String, format: String): Any = str
    }

    object long extends PrimitiveType("long") {
      def fromString(str: String, format: String): Any = if (str == null || str.isEmpty) null else str.toLong
    }

    object double extends PrimitiveType("double") {
      def fromString(str: String, format: String): Any = if (str == null || str.isEmpty) null else str.toDouble
    }

    object boolean extends PrimitiveType("boolean") {
      def fromString(str: String, format: String): Any = if (str == null || str.isEmpty) null else str.toBoolean
    }

    object byte extends PrimitiveType("byte") {
      def fromString(str: String, format: String): Any = if (str == null || str.isEmpty) null else str.toByte
    }


    private def instantFromString(str: String, format: String): Instant = {
      import java.time.format.DateTimeFormatter
      val formatter = DateTimeFormatter.ofPattern(format)
      val dateTime = formatter.parse(str)
      Try(Instant.from(dateTime)) match {
        case Success(instant) =>
          instant

        case Failure(ex) =>
          val localDateTime = LocalDateTime.from(dateTime)
          ZonedDateTime.of(localDateTime, ZoneId.systemDefault()).toInstant
      }
    }

    object date extends PrimitiveType("date") {
      def fromString(str: String, format: String): Any = {
        if (str == null || str.isEmpty)
          null
        else {
          val instant = instantFromString(str, format)
          new java.sql.Date(instant.toEpochMilli)

        }
      }
    }

    object timestamp extends PrimitiveType("timestamp") {
      def fromString(str: String, format: String): Any = {
        if (str == null || str.isEmpty)
          null
        else {
          val instant = instantFromString(str, format)
          Timestamp.from(instant)
        }
      }
    }

  }

  case class Types(types: List[Type]) {
  }

  case class Type(name: String, primitiveType: PrimitiveType, pattern: Pattern) {
    def matches(value: String): Boolean = {
      pattern.matcher(name).matches()
    }

    def sparkType(fieldName: String, nullable: Boolean): StructField = {
      StructField(fieldName, CatalystSqlParser.parseDataType(primitiveType.value), nullable)
    }

  }

  case class DSVAttribute(name: String,
                          `type`: String,
                          required: Boolean,
                          privacy: PrivacyLevel)

  case class Schema(name: String,
                    pattern: Pattern,
                    attributes: List[DSVAttribute],
                    metadata: Option[Metadata])

  case class Domain(name: String,
                    directory: String,
                    metadata: Metadata,
                    schemas: List[Schema]) {
    def findSchema(filename: String): Option[Schema] = {
      schemas.find(_.pattern.matcher(filename).matches())
    }
  }


  @JsonDeserialize(using = classOf[MetadataDeserializer])
  case class Metadata(
                       mode: Option[Mode] = None,
                       format: Option[Format] = None,
                       withHeader: Option[Boolean] = None,
                       separator: Option[String] = None,
                       quote: Option[String] = None,
                       escape: Option[String] = None,
                       write: Option[Write] = None,
                       dateFormat: Option[String] = None,
                       timestampFormat: Option[String] = None
                     ) {

    def getMode(): Mode = mode.getOrElse(FILE)

    def getFormat(): Format = format.getOrElse(DSV)

    def isWithHeader(): Boolean = withHeader.getOrElse(true)

    def getSeparator(): String = separator.getOrElse(";")

    def getQuote(): String = quote.getOrElse("\"")

    def getEscape(): String = escape.getOrElse("\\")

    def getWrite(): Write = write.getOrElse(APPEND)

    def merge(child: Metadata): Metadata = {
      def defined[T](parent: Option[T], child: Option[T]): Option[T] =
        if (child.isDefined) child else parent

      Metadata(
        defined(this.mode, child.mode),
        defined(this.format, child.format),
        defined(this.withHeader, child.withHeader),
        defined(this.separator, child.separator),
        defined(this.quote, child.quote),
        defined(this.escape, child.escape),
        defined(this.write, child.write),
        defined(this.dateFormat, child.dateFormat),
        defined(this.timestampFormat, child.timestampFormat)
      )
    }
  }


  object Metadata {
    def Dsv(
             separator: Option[String],
             quote: Option[String],
             escape: Option[String],
             write: Option[Write]
           ) = new Metadata(
      Some(Mode.FILE),
      Some(Format.DSV),
      Some(true),
      separator,
      quote,
      escape,
      write,
      Some("yyyy-MM-dd"),
      Some("yyyy-MM-dd HH:mm:ss")
    )
  }


  class MetadataDeserializer extends JsonDeserializer[Metadata] {
    override def deserialize(jp: JsonParser, ctx: DeserializationContext): Metadata = {
      val node: JsonNode = jp.getCodec().readTree(jp)
      val mode = if (node.get("mode").isNull) None else Some(Mode.fromString(node.get("mode").asText))
      val format = if (node.get("format").isNull) None else Some(Format.fromString(node.get("format").asText))
      val withHeader = if (node.get("withHeader").isNull) None else Some(node.get("withHeader").asBoolean())
      val separator = if (node.get("separator").isNull) None else Some(node.get("separator").asText)
      val quote = if (node.get("quote").isNull) None else Some(node.get("quote").asText)
      val escape = if (node.get("escape").isNull) None else Some(node.get("escape").asText)
      val write = if (node.get("write").isNull) None else Some(Write.fromString(node.get("write").asText))
      val dateFormat = if (node.get("dateFormat").isNull) None else Some(node.get("dateFormat").asText)
      val timestampFormat = if (node.get("timestampFormat").isNull) None else Some(node.get("timestampFormat").asText)
      Metadata(mode, format, withHeader, separator, quote, escape, write, dateFormat, timestampFormat)
    }
  }

  def Json(separator: Option[String],
           quote: Option[String],
           escape: Option[String],
           write: Option[Write]) = new Metadata(
    Some(Mode.FILE),
    Some(Format.JSON),
    Some(true),
    separator,
    quote,
    escape,
    write,
    Some("yyyy-MM-dd"),
    Some("yyyy-MM-dd HH:mm:ss")
  )

  def Stream(
              separator: Option[String],
              quote: Option[String],
              escape: Option[String]) = new Metadata(
    Some(Mode.STREAM),
    Some(Format.JSON),
    Some(true),
    separator,
    quote,
    escape,
    Some(Write.APPEND),
    Some("yyyy-MM-dd"),
    Some("yyyy-MM-dd HH:mm:ss")
  )


  /**
    * 
    * @param sql SQL request to exexute (do not forget to prefix table names with the database name
    * @param domain Output domain in Business Area (Will be the Database name in Hive)
    * @param dataset Dataset Name in Business Area (Will be the Table name in Hive)
    * @param write   Append to or overwrite existing data
    */
  case class BusinessTask(sql: String, domain: String, dataset: String, write: Write)

  /**
    *
    * @param name Buisiness Job logical name
    * @param cron All business task will be executed at this time
    * @param tasks List of business tasks to execute
    */
  case class BusinessJob(name: String, tasks: List[BusinessTask])

}
