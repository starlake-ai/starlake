package com.ebiznext.comet.schema.model

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.regex.Pattern

import com.ebiznext.comet.schema.model.SchemaModel.Format.DSV
import com.ebiznext.comet.schema.model.SchemaModel.Mode.FILE
import com.ebiznext.comet.schema.model.SchemaModel.Write.APPEND
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructField
import org.json4s.{CustomSerializer, DefaultFormats, JNull, JString}

import scala.util.{Failure, Success, Try}

object SchemaModel {
  val formats = DefaultFormats + FormatSerializer + ModeSerializer + WriteSerializer + PrivacyLevelSerializer + PatternSerializer + PathSerializer + PrimitiveTypeSerializer

  sealed case class Mode(value: String)

  object Mode {

    object FILE extends Mode("FILE")

    object STREAM extends Mode("STREAM")

  }

  sealed case class Write(value: String)

  object Write {

    object OVERWRITE extends Write("OVERWRITE")

    object APPEND extends Write("APPEND")

  }

  sealed case class Format(value: String)

  object Format {

    object DSV extends Format("DSV")

    object JSON extends Format("JSON")

  }

  case object ModeSerializer
    extends CustomSerializer[Mode](_ =>
      ( {
        case JString(data) =>
          data match {
            case "FILE" => Mode.FILE
            case "STREAM" => Mode.STREAM
          }
        case JNull => null
      }, {
        case mode: Mode =>
          JString(mode.value)
      }))

  case object WriteSerializer
    extends CustomSerializer[Write](_ =>
      ( {
        case JString(data) =>
          data match {
            case "OVERWRITE" => Write.OVERWRITE
            case "APPEND" => Write.APPEND
          }
        case JNull => null
      }, {
        case write: Write =>
          JString(write.value)
      }))

  case object FormatSerializer
    extends CustomSerializer[Format](_ =>
      ( {
        case JString(data) =>
          data match {
            case "DSV" => Format.DSV
            case "JSON" => Format.JSON
          }
        case JNull => null
      }, {
        case format: Format =>
          JString(format.value)
      }))

  sealed case class PrivacyLevel(value: String)

  object PrivacyLevel {

    object NONE extends PrivacyLevel("NONE")

    object HIDE extends PrivacyLevel("HIDE")

    object MD5 extends PrivacyLevel("MD5")

    object SHA1 extends PrivacyLevel("SHA1")

    object AES extends PrivacyLevel("AES")

  }

  case object PathSerializer
    extends CustomSerializer[Path](_ =>
      ( {
        case JString(data) =>
          new Path(data)
        case JNull => null
      }, {
        case path: Path =>
          JString(path.toString)
      }))


  case object PrivacyLevelSerializer
    extends CustomSerializer[PrivacyLevel](_ =>
      ( {
        case JString(data) =>
          data match {
            case "NONE" => PrivacyLevel.NONE
            case "HIDE" => PrivacyLevel.HIDE
            case "MD5" => PrivacyLevel.MD5
            case "SHA1" => PrivacyLevel.SHA1
            case "AES" => PrivacyLevel.AES
          }
        case JNull => null
      }, {
        case privacy: PrivacyLevel =>
          JString(privacy.value)
      }))

  sealed abstract case class PrimitiveType(value: String) {
    def fromString(str: String, format: String = null): Any
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

  case object PrimitiveTypeSerializer
    extends CustomSerializer[PrimitiveType](_ =>
      ( {
        case JString(data) =>
          data match {
            case "string" => PrimitiveType.string
            case "long" => PrimitiveType.long
            case "double" => PrimitiveType.double
            case "boolean" => PrimitiveType.boolean
            case "byte" => PrimitiveType.byte
            case "date" => PrimitiveType.date
            case "timestamp" => PrimitiveType.timestamp
          }
        case JNull => null
      }, {
        case primitive: PrimitiveType =>
          JString(primitive.value)
      }))

  case class Types(types: List[Type]) {
  }

  case class Type(name: String, primitiveType: PrimitiveType, pattern: Pattern) {
    def matches(value: String): Boolean = {
      pattern.matcher(name).matches()
    }

    def sparkType(fieldName:String, nullable: Boolean): StructField = {
      StructField(fieldName, CatalystSqlParser.parseDataType(primitiveType.value), nullable)
    }

  }

  case object PatternSerializer
    extends CustomSerializer[Pattern](_ =>
      ( {
        case JString(data) =>
          Pattern.compile(data)

        case JNull => null
      }, {
        case pattern: Pattern =>
          JString(pattern.pattern())
      }))

  case class DSVAttribute(name: String,
                          `type`: String,
                          required: Boolean,
                          privacy: PrivacyLevel)

  case class Schema(name: String,
                    pattern: Pattern,
                    attributes: List[DSVAttribute],
                    metadata: Metadata)

  case class Domain(name: String,
                    directory: String,
                    metadata: Metadata,
                    schemas: List[Schema]) {
    def findSchema(filename: String): Option[Schema] = {
      schemas.find(_.pattern.matcher(filename).matches())
    }
    def directoryPath: Path = new Path(directory)
  }

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
}

/*
"metadata": {
"source": "EBOUTIQUE",
"fileCode": "EBOUTIQUE_ADDRESSES",
"nameFormat": "ADDRESSES-.*\\.dsv\\.bz2",
"fileFormat": "CSV",
"separator": "|",
"isReferential": false,
"dataType": "cold",
"dateFormat": "yyyy-MM-dd",
"timestampFormat": "yyyy-MM-dd HH:mm:ss"
}
 */
