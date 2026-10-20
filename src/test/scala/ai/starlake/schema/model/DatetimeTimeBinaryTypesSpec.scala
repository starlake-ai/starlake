package ai.starlake.schema.model

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils.SqlColumn
import ai.starlake.schema.model.PrimitiveType.{binary, datetime, time}
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{BinaryType, StringType, TimestampNTZType}

import java.time.LocalDateTime
import scala.io.Source
import scala.util.Try

class DatetimeTimeBinaryTypesSpec extends TestHelper {

  "datetime" should "parse a civil date and time with its pattern" in {
    datetime.fromString("2019-12-31 23:59:02", "yyyy-MM-dd HH:mm:ss") shouldBe
    LocalDateTime.of(2019, 12, 31, 23, 59, 2)
    datetime.fromString(" 2019-12-31T23:59:02.5 ", "ISO_LOCAL_DATE_TIME") shouldBe
    LocalDateTime.of(2019, 12, 31, 23, 59, 2, 500000000)
    datetime.fromString("2019-12-31T23:59:02", "ISO_DATE_TIME") shouldBe
    LocalDateTime.of(2019, 12, 31, 23, 59, 2)
    datetime.sparkType(None) shouldBe TimestampNTZType
    Option(datetime.fromString(null, "ISO_LOCAL_DATE_TIME")) shouldBe None
    Option(datetime.fromString("", "ISO_LOCAL_DATE_TIME")) shouldBe None
  }

  it should "read the SQL text of a date and time, with a space or a T" in {
    datetime.fromString("2019-12-31 23:59:02.123456", "SQL_DATE_TIME") shouldBe
    LocalDateTime.of(2019, 12, 31, 23, 59, 2, 123456000)
    datetime.fromString("2019-12-31T23:59:02", "SQL_DATE_TIME") shouldBe
    LocalDateTime.of(2019, 12, 31, 23, 59, 2)
    datetime.fromString("2019-12-31 23:59", "SQL_DATE_TIME") shouldBe
    LocalDateTime.of(2019, 12, 31, 23, 59)
    Try(datetime.fromString("2019-02-30 00:00:00", "SQL_DATE_TIME")).isFailure shouldBe true
    Try(datetime.fromString("2019-12-31", "SQL_DATE_TIME")).isFailure shouldBe true
  }

  it should "reject the years out of 1 to 9999" in {
    Try(datetime.fromString("0000-01-01T00:00:00", "ISO_LOCAL_DATE_TIME")).isFailure shouldBe true
    Try(datetime.fromString("+12019-12-31T00:00:00", "ISO_LOCAL_DATE_TIME")).isFailure shouldBe true
    datetime.fromString("9999-12-31T23:59:59", "ISO_LOCAL_DATE_TIME") shouldBe
    LocalDateTime.of(9999, 12, 31, 23, 59, 59)
  }

  it should "start a date without time at the start of the day" in {
    datetime.fromString("2019-12-31", "ISO_LOCAL_DATE") shouldBe LocalDateTime.of(2019, 12, 31, 0,
      0)
    datetime.fromString("31/12/2019", "dd/MM/yyyy") shouldBe LocalDateTime.of(2019, 12, 31, 0, 0)
  }

  it should "reject a value with an offset or a time zone" in {
    Try(datetime.fromString("2019-12-31T23:59:02+01:00", "ISO_DATE_TIME")).isFailure shouldBe true
    Try(datetime.fromString("2019-12-31+01:00", "ISO_DATE")).isFailure shouldBe true
    Try(
      datetime.fromString("2019-12-31 23:59:02 Europe/Paris", "yyyy-MM-dd HH:mm:ss VV")
    ).isFailure shouldBe true
    Try(datetime.fromString("2019-12-31 23:59", "yyyy-MM-dd HH:mm:ss")).isFailure shouldBe true
    // A time without AM or PM is not the start of the day
    Try(datetime.fromString("2019-12-31 11:45:00", "yyyy-MM-dd hh:mm:ss")).isFailure shouldBe true
  }

  "time" should "give the text of the time of day, with the microseconds when they are not 0" in {
    time.fromString("23:59:02", "ISO_LOCAL_TIME") shouldBe "23:59:02"
    time.fromString("12:00", "ISO_LOCAL_TIME") shouldBe "12:00:00"
    time.fromString("23:59:02.5", "ISO_LOCAL_TIME") shouldBe "23:59:02.500000"
    // Digits beyond the microsecond are dropped
    time.fromString("23:59:02.123456789", "ISO_LOCAL_TIME") shouldBe "23:59:02.123456"
    time.fromString("23:59:02.000000400", "ISO_LOCAL_TIME") shouldBe "23:59:02"
    time.fromString("07h08", "HH'h'mm") shouldBe "07:08:00"
    time.fromString("10:15:30", "ISO_TIME") shouldBe "10:15:30"
    time.sparkType(None) shouldBe StringType
    Option(time.fromString("", "ISO_LOCAL_TIME")) shouldBe None
  }

  it should "reject an invalid time or a time with an offset" in {
    Try(time.fromString("25:00:00", "ISO_LOCAL_TIME")).isFailure shouldBe true
    Try(time.fromString("10:15:30+01:00", "ISO_TIME")).isFailure shouldBe true
  }

  "binary" should "decode base64 and hex texts" in {
    val hello = "Hello".getBytes("UTF-8").toList
    binary.fromString("SGVsbG8=", "base64").asInstanceOf[Array[Byte]].toList shouldBe hello
    binary.fromString("48656c6c6f", "hex").asInstanceOf[Array[Byte]].toList shouldBe hello
    binary.fromString("48656C6C6F", "HEX").asInstanceOf[Array[Byte]].toList shouldBe hello
    binary.fromString("00ff", "hex").asInstanceOf[Array[Byte]].toList shouldBe List[Byte](0, -1)
    // Line breaks of MIME or XML are not part of the bytes
    binary.fromString("SGVs\r\nbG8=", "base64").asInstanceOf[Array[Byte]].toList shouldBe hello
    binary.sparkType(None) shouldBe BinaryType
    Option(binary.fromString("", "base64")) shouldBe None
  }

  it should "reject a text that is not in its encoding" in {
    Try(binary.fromString("not base64!", "base64")).isFailure shouldBe true
    Try(binary.fromString("4865F", "hex")).isFailure shouldBe true
    Try(binary.fromString("zz", "hex")).isFailure shouldBe true
    Try(binary.fromString("-1-1", "hex")).isFailure shouldBe true
    Try(binary.fromString("+f+f", "hex")).isFailure shouldBe true
    Try(binary.fromString("SGVsbG8=", "utf8")).isFailure shouldBe true
  }

  "PrimitiveType.from" should "map the Spark TIMESTAMP_NTZ and binary types" in {
    PrimitiveType.from(TimestampNTZType) shouldBe datetime
    PrimitiveType.from(BinaryType) shouldBe binary
  }

  "The datetime, time and binary types" should "be valid with a valid pattern only" in {
    def valid(primitiveType: PrimitiveType, pattern: String, sample: Option[String] = None) =
      Type("t", pattern, primitiveType, sample = sample).checkValidity().isRight

    valid(datetime, "yyyy-MM-dd HH:mm:ss", Some("2019-12-31 23:59:02")) shouldBe true
    valid(datetime, "SQL_DATE_TIME", Some("2019-12-31 23:59:02.123456")) shouldBe true
    valid(datetime, "ISO_LOCAL_DATE_TIME") shouldBe true
    valid(datetime, "ISO_DATE_TIME") shouldBe true
    valid(datetime, "dd/MM/yyyy") shouldBe true
    valid(datetime, "ISO_WEEK_DATE", Some("2012-W48-6")) shouldBe true
    // An epoch, an offset, a time zone or a time without date is not a civil date and time
    valid(datetime, "epoch_second") shouldBe false
    valid(datetime, "ISO_OFFSET_DATE_TIME") shouldBe false
    valid(datetime, "ISO_INSTANT") shouldBe false
    valid(datetime, "yyyy-MM-dd'T'HH:mm:ssXXX") shouldBe false
    valid(datetime, "yyyy-MM-dd HH:mm:ss VV") shouldBe false
    valid(datetime, "yyyy-MM-dd HH:mm:ss z") shouldBe false
    valid(datetime, "HH:mm:ss") shouldBe false
    valid(datetime, "yyyy-MM-dd hh:mm:ss") shouldBe false
    valid(datetime, "yyyy-MM-dd hh:mm:ss a") shouldBe true
    valid(datetime, "yyyy-MM-dd HH:mm") shouldBe true
    valid(datetime, "yyyy-MM-dd HH:mm:ss", Some("2019-12-31")) shouldBe false
    valid(time, "ISO_LOCAL_TIME", Some("23:59:02.123")) shouldBe true
    valid(time, "ISO_TIME") shouldBe true
    valid(time, "HH:mm") shouldBe true
    valid(time, "hh:mm a") shouldBe true
    valid(time, "ISO_OFFSET_TIME") shouldBe false
    valid(time, "HH:mm:ssXXX") shouldBe false
    valid(time, "yyyy-MM-dd") shouldBe false
    // The hour of the day is unknown without AM or PM
    valid(time, "hh:mm") shouldBe false
    valid(binary, "base64", Some("SGVsbG8=")) shouldBe true
    valid(binary, "hex", Some("48656c6c6f")) shouldBe true
    valid(binary, "base64", Some("48656c6c6f!")) shouldBe false
    valid(binary, "utf8") shouldBe false
  }

  "The built-in types of extra.sl.yml" should "be valid against the JSON schema and their samples" in {
    val extraTypes =
      YamlSerde.deserializeYamlTypes(
        Source.fromResource("types/extra.sl.yml").mkString,
        "extra.sl.yml"
      )
    extraTypes.map(tpe => tpe.name -> tpe.primitiveType) shouldBe List(
      "datetime" -> datetime,
      "time"     -> time,
      "binary"   -> binary
    )
    TypesInfo(1, extraTypes).checkValidity() shouldBe Right(true)
    // The native loaders need a mapping for their engine
    extraTypes.foreach { tpe =>
      tpe.ddlMapping
        .getOrElse(Map.empty)
        .keySet should contain allOf ("bigquery", "duckdb", "snowflake")
    }
  }

  new WithSettings() {
    // The test project has a copy of default.sl.yml, and a types.sl.yml defining its own datetime
    // (a timestamp) and time (a string)
    "A project defining datetime and time" should "keep its types and get the binary type last" in {
      cleanMetadata
      val schemaHandler = settings.schemaHandler(reload = true)
      val types = schemaHandler.types(reload = true)
      types.find(_.name == "datetime").map(_.primitiveType) shouldBe Some(PrimitiveType.timestamp)
      types.find(_.name == "time").map(_.primitiveType) shouldBe Some(PrimitiveType.string)
      types.map(_.name).count(Set("datetime", "time", "binary")) shouldBe 3
      types.last.name shouldBe "binary"
      types.last.primitiveType shouldBe binary
      // A search by primitive type still finds the types of the project first
      types.find(_.primitiveType == PrimitiveType.timestamp).map(_.name) shouldBe Some("timestamp")
      types.find(_.primitiveType == PrimitiveType.string).map(_.name) shouldBe Some("string")
      // The types of the project are used to find the type of a column, not the built-in binary
      schemaHandler.typesForSqlTypeLookup() shouldBe types.init
      // Reported by validate
      schemaHandler.replacedExtraTypes().map { case (extraType, projectType) =>
        extraType.name -> projectType.primitiveType
      } shouldBe List("datetime" -> PrimitiveType.timestamp, "time" -> PrimitiveType.string)
      // No type of the project is a datetime: infer-schema keeps the name, with a warning
      schemaHandler.typeNameOf(datetime) shouldBe None
      schemaHandler.typeNameOf(binary) shouldBe Some("binary")
    }
  }

  new WithSettings() {
    private def projectWithoutDatetimeAndTime() = {
      cleanMetadata
      deliverTestFile("/sample/new-types/types.sl.yml", new Path(DatasetArea.types, "types.sl.yml"))
      settings.schemaHandler(reload = true)
    }

    "A project without datetime and time types" should "get the built-in ones after its own types" in {
      val schemaHandler = projectWithoutDatetimeAndTime()
      val types = schemaHandler.types(reload = true)
      types.takeRight(3).map(tpe => tpe.name -> tpe.primitiveType) shouldBe List(
        "datetime" -> datetime,
        "time"     -> time,
        "binary"   -> binary
      )
      // The types of the project, unchanged, come first
      types.dropRight(3).takeRight(2).map(_.name) shouldBe List("iso_civil", "hex_bytes")
      schemaHandler.replacedExtraTypes() shouldBe Nil
      // The built-in type, not the first type of the project of the same primitive type
      schemaHandler.typeNameOf(datetime) shouldBe Some("datetime")
      schemaHandler.typeNameOf(binary) shouldBe Some("binary")
    }

    "An XML or Kafka load" should "read the datetime, time and binary values as text at every level" in {
      val schemaHandler = projectWithoutDatetimeAndTime()
      val untyped = SchemaInfo(
        "t",
        java.util.regex.Pattern.compile("t"),
        List(
          TableAttribute("civil", "datetime"),
          TableAttribute("walls", "time", array = Some(true)),
          TableAttribute("count", "long"),
          TableAttribute(
            "record",
            "struct",
            attributes = List(
              TableAttribute("payload", "binary"),
              TableAttribute("days", "datetime", array = Some(true)),
              TableAttribute("count", "long")
            )
          )
        )
      ).sourceSparkSchemaUntypedEpochWithoutScriptedFields(schemaHandler)
      import org.apache.spark.sql.types._
      untyped.fields.map(field => field.name -> field.dataType).toList shouldBe List(
        "civil" -> StringType,
        "walls" -> ArrayType(StringType, containsNull = true),
        "count" -> LongType,
        "record" -> StructType(
          List(
            StructField("payload", StringType),
            StructField("days", ArrayType(StringType, containsNull = true)),
            StructField("count", LongType)
          )
        )
      )
    }

    "The type of a Snowflake column" should "stay the one it had before the built-in types existed" in {
      val schemaHandler = projectWithoutDatetimeAndTime()
      def typeOf(sqlType: String): PrimitiveType =
        PrimitiveType.fromSQLType(
          "snowflake",
          SqlColumn("column", sqlType, None, None),
          schemaHandler
        )
      // A TIMESTAMP column is a TIMESTAMP_NTZ, the Snowflake mapping of datetime
      typeOf("TIMESTAMP_NTZ") shouldBe PrimitiveType.timestamp
      typeOf("TIME") shouldBe PrimitiveType.string
      typeOf("BINARY") shouldBe PrimitiveType.string
      typeOf("DATE") shouldBe PrimitiveType.date
    }

    it should "stay the same in a project holding a copy of the built-in types" in {
      cleanMetadata
      deliverTestFile("/types/extra.sl.yml", new Path(DatasetArea.types, "types.sl.yml"))
      val schemaHandler = settings.schemaHandler(reload = true)
      schemaHandler.types(reload = true).count(_.name == "datetime") shouldBe 1
      schemaHandler.replacedExtraTypes() shouldBe Nil
      PrimitiveType.fromSQLType(
        "snowflake",
        SqlColumn("column", "TIMESTAMP_NTZ", None, None),
        schemaHandler
      ) shouldBe PrimitiveType.timestamp
    }
  }
}
