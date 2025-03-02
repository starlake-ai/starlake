package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.schema.model.Attribute

class InferSchemaHandlerSpec extends TestHelper {

  new WithSettings() {
    "CreateAttributes" should "create the correct list of attributes for a complex Json" in {
      val sparkImplicits = sparkSession.implicits
      import sparkImplicits._

      val ComplexjsonStr = """{ "metadata": { "key": 84896, "value": 54, "values": [1, 2, 3] }}"""

      val df = sparkSession.read
        .option("inferSchema", value = true)
        .json(Seq(ComplexjsonStr).toDS())

      val complexAttr2 = Attribute("key", "long", Some(false), required = None)
      val complexAttr3 =
        Attribute("value", "long", Some(false), required = None)
      val complexAttr4 = Attribute("values", "long", Some(true), required = None)

      val complexAttr1: List[Attribute] = List(
        Attribute(
          "metadata",
          "struct",
          Some(false),
          required = None,
          attributes = List(complexAttr2, complexAttr3, complexAttr4)
        )
      )

      val complexAttr: List[Attribute] =
        InferSchemaHandler.createAttributes(Map.empty, df.schema)

      complexAttr shouldBe complexAttr1
    }

    "CreateAttributes" should "create the correct list of attributes for a simple Json" in {
      val sparkImplicits = sparkSession.implicits
      import sparkImplicits._

      val SimpleJsonStr = """{ "key": "Fares", "value": 3 }}"""

      val df1 = sparkSession.read
        .option("inferSchema", value = true)
        .json(Seq(SimpleJsonStr).toDS())

      val simpleAttr: List[Attribute] =
        InferSchemaHandler.createAttributes(Map.empty, df1.schema)

      val simpleAttr1: List[Attribute] = List(
        Attribute("key", "string", Some(false), required = None),
        Attribute("value", "long", Some(false), required = None)
      )
      simpleAttr shouldBe simpleAttr1
    }

    "CreateAttributes" should "create the correct list of attributes for an array Json" in {
      val sparkImplicits = sparkSession.implicits
      import sparkImplicits._

      val arrayJson =
        """
          |[
          |	{
          |		"id" : 1,
          |		"name" : ["New York", "NY"]
          |	},
          |	{
          |		"id" : 2,
          |		"name" : ["Paris"]
          |	}
          |]
          |""".stripMargin

      val df1 = sparkSession.read
        .option("inferSchema", value = true)
        .json(Seq(arrayJson).toDS())

      val arrayAttr: List[Attribute] =
        InferSchemaHandler.createAttributes(Map.empty, df1.schema)

      val arrayAttr1: List[Attribute] = List(
        Attribute("id", "long", Some(false), required = None),
        Attribute("name", "string", Some(true), required = None)
      )

      arrayAttr shouldBe arrayAttr1

    }

    "CreateAttributes" should "create the correct list of attributes for a dsv with header" in {
      val df1 = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", value = true)
        .option("header", value = true)
        .option("delimiter", ";")
        .option("parserLib", "UNIVOCITY")
        .load("src/test/resources/sample/SCHEMA-VALID.dsv")

      val dsv: List[Attribute] =
        InferSchemaHandler.createAttributes(Map.empty, df1.schema, forcePattern = false)

      val dsv1: List[Attribute] = List(
        Attribute("first name", "string", Some(false), required = None),
        Attribute("last name", "string", Some(false), required = None),
        Attribute("age", "string", Some(false), required = None),
        Attribute("ok", "string", Some(false), required = None)
      )
      dsv shouldBe dsv1

    }

    "CreateAttributes" should "create the correct list of attributes for a dsv without header" in {
      val df1 = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", value = true)
        .option("header", value = false)
        .option("delimiter", ";")
        .option("parserLib", "UNIVOCITY")
        .load("src/test/resources/sample/SCHEMA-VALID-NOHEADER.dsv")

      val dsv: List[Attribute] = InferSchemaHandler.createAttributes(Map.empty, df1.schema)

      val dsv1: List[Attribute] = List(
        Attribute("_c0", "string", Some(false), required = None),
        Attribute("_c1", "string", Some(false), required = None),
        Attribute("_c2", "int", Some(false), required = None)
      )
      dsv shouldBe dsv1

    }
    "CreateXML Attributes with - or : chars" should "create the correct list of attributes for a XML without header" in {
      val df1 = sparkSession.read
        .format("com.databricks.spark.xml")
        .option("inferSchema", value = true)
        .option("rowTag", "catalog")
        .option("ignoreNamespace", "true")
        .load("src/test/resources/sample/SAMPLE-XML-SPECIAL-CHARS.xml")
      InferSchemaHandler.createAttributes(Map.empty, df1.schema)

      df1.schema.printTreeString()

    }
  }
}
