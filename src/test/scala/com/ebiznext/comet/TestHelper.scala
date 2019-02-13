package com.ebiznext.comet

import java.io.{File, InputStream}
import java.nio.file.Files
import java.time.LocalDate
import java.util.Calendar
import java.util.regex.Pattern

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.handlers.{HdfsStorageHandler, SchemaHandler}
import com.ebiznext.comet.schema.model._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.io.Source

trait TestHelper extends FlatSpec with Matchers with BeforeAndAfterAll {

  /**
    * types:
    * - name: "string"
    * primitiveType: "string"
    * pattern: ".+"
    * - name: "time"
    * primitiveType: "string"
    * pattern: "(1[012]|[1-9]):[0-5][0-9](\\\\s)?(?i)(am|pm)"
    * - name: "time24"
    * primitiveType: "string"
    * pattern: "([01]?[0-9]|2[0-3]):[0-5][0-9]"
    * - name: "date"
    * primitiveType: "date"
    * pattern: "(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\\\d\\\\d)"
    * - name: "username"
    * primitiveType: "string"
    * pattern: "[a-z0-9_-]{3,15}"
    * - name: "age"
    * primitiveType: "long"
    * pattern: "[0-9]{1,15}"
    * - name: "color"
    * primitiveType: "string"
    * pattern: "#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})"
    * - name: "ip"
    * primitiveType: "string"
    * pattern: "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])"
    * - name: "email"
    * primitiveType: "string"
    * pattern: "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,6}"
    */
  def loadFile(filename: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(filename)
    scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def readFileContent(path: String): String =
    Source.fromFile(path).getLines.mkString("\n")

  def readFileContent(path: Path): String = readFileContent(path.toUri.getPath)

  def getResPath(path: String): String = getClass.getResource(path).toURI.getPath

  def prepareDateColumns(df: DataFrame): DataFrame = {

    df.withColumn("comet_date", current_date())
      .withColumn("year", year(col("comet_date")))
      .withColumn("month", month(col("comet_date")))
      .withColumn("day", dayofmonth(col("comet_date")))
      .drop("comet_date")
  }

  def prepareSchema(schema: StructType): StructType =
    StructType(schema.fields.filterNot(f => List("year", "month", "day").contains(f.name)))

  def getTodayPartitionPath: String = {
    val now = LocalDate.now
    s"year=${now.getYear}/month=${now.getMonthValue}/day=${now.getDayOfMonth}"
  }

  def cleanMetadata = (new File(cometMetadataPath)).listFiles().map(_.delete())

  val types = Types(
    List(
      Type("string", ".+", PrimitiveType.string),
      Type("time", "(1[012]|[1-9]):[0-5][0-9](\\\\s)?(?i)(am|pm)"),
      Type("time24", "([01]?[0-9]|2[0-3]):[0-5][0-9]"),
      Type(
        "data",
        "(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\\\d\\\\d)",
        PrimitiveType.date
      ),
      Type("username", "[a-z0-9_-]{3,15}"),
      Type("age", "[a-z0-9_-]{3,15}", PrimitiveType.long),
      Type("color", "#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})", PrimitiveType.string),
      Type(
        "ip",
        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])"
      ),
      Type("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,6}")
    )
  )

  val domain = Domain(
    "DOMAIN",
    "/tmp/incoming/DOMAIN",
    Some(
      Metadata(
        Some(Mode.FILE),
        Some(Format.DSV),
        Some(false),
        Some(false),
        Some(false),
        Some(";"),
        Some("\""),
        Some("\\"),
        Some(WriteMode.APPEND),
        None
      )
    ),
    List(
      Schema(
        "User",
        Pattern.compile("SCHEMA-.*.dsv"),
        List(
          Attribute("firstname", "string", Some(false), false, Some(PrivacyLevel.NONE)),
          Attribute("lastname", "string", Some(false), false, Some(PrivacyLevel.SHA1)),
          Attribute("age", "age", Some(false), false, Some(PrivacyLevel.HIDE))
        ),
        Some(Metadata(withHeader = Some(true))),
        None,
        Some("Schema Comment"),
        Some(List("SQL1", "SQL2")),
        None
      )
    ),
    Some("Domain Comment")
  )

  val mapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
  // provides all of the Scala goodiness
  mapper.registerModule(DefaultScalaModule)

  val storageHandler = new HdfsStorageHandler
  val schemaHandler = new SchemaHandler(storageHandler)

  lazy val tempFile = (Files.createTempDirectory("comet")).toString

  lazy val cometDatasetsPath = tempFile + "/datasets"
  lazy val cometMetadataPath = tempFile + "/metadata"

  lazy val sparkSession = SparkSession.builder
    .master("local[*]")
    .getOrCreate

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Init
    System.setProperty("COMET_DATASETS", cometDatasetsPath)
    System.setProperty("COMET_METADATA", cometMetadataPath)

    new File("/tmp/DOMAIN").mkdir()
    new File("/tmp/dream").mkdir()

    DatasetArea.init(storageHandler)
  }
  override protected def afterAll(): Unit = {
    super.afterAll()
    sparkSession.stop()
  }
}
