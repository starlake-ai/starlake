package com.ebiznext.comet.schema.handlers
import com.ebiznext.comet.schema.model.Attribute
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class InferSchemaHandlerSpec extends FlatSpec with Matchers{


  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("InferSchemaHandlerTest")
    .getOrCreate()

  import spark.implicits._



  "CreateAttributes" should "create the correct list of attributes for a complex Json" in {


    val ComplexjsonStr = """{ "metadata": { "key": 84896, "value": 54 }}"""

    val df = spark.read
      .option("inferSchema", value = true)
      .json(Seq(ComplexjsonStr).toDS.rdd)

  val complexAttr2 = Attribute("key", "long", Some(false), false)
  val complexAttr3 = Attribute("value", "long", Some(false), false)

  val complexAttr1: List[Attribute] = List(
    Attribute(
      "metadata",
      "struct",
      Some(false),
      false,
      attributes = Some(List(complexAttr2, complexAttr3))
    )
  )

  val complexAttr: List[Attribute] = InferSchemaHandler.createAttributes(df.schema)

    complexAttr shouldBe complexAttr1
  }

  "CreateAttributes" should "create the correct list of attributes for a simple Json" in {
    val SimpleJsonStr = """{ "key": "Fares", "value": 3 }}"""

    val df1 = spark.read
    .option("inferSchema", value = true)
    .json(Seq(SimpleJsonStr).toDS.rdd)

  val simpleAttr: List[Attribute] = InferSchemaHandler.createAttributes(df1.schema)

  val simpleAttr1: List[Attribute] = List(
    Attribute("key", "string", Some(false), false),
    Attribute("value", "long", Some(false), false)
  )
    simpleAttr shouldBe simpleAttr1
  }



  "CreateAttributes" should "create the correct list of attributes for a dsv with header" in {
    val df1 = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", value = true)
      .option("header", true)
      .option("delimiter", ";")
      .option("parserLib", "UNIVOCITY")
      .load("src/test/resources/sample/SCHEMA-VALID.dsv")

    val dsv: List[Attribute] = InferSchemaHandler.createAttributes(df1.schema)

    val dsv1: List[Attribute] = List(
      Attribute("firstname", "string", Some(false), false),
      Attribute("lastname", "string", Some(false), false),
      Attribute("age", "string", Some(false), false)
    )
    dsv shouldBe dsv1

  }

  "CreateAttributes" should "create the correct list of attributes for a dsv without header" in {
    val df1 = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", value = true)
      .option("header", false)
      .option("delimiter", ";")
      .option("parserLib", "UNIVOCITY")
      .load("src/test/resources/sample/SCHEMA-VALID-NOHEADER.dsv")

    val dsv: List[Attribute] = InferSchemaHandler.createAttributes(df1.schema)

    val dsv1: List[Attribute] = List(
      Attribute("_c0", "string", Some(false), false),
      Attribute("_c1", "string", Some(false), false),
      Attribute("_c2", "integer", Some(false), false)
    )
    dsv shouldBe dsv1

  }

}
