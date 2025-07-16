package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.schema.model.TableAttribute

class InferSchemaInfoHandlerSpec extends TestHelper {

  new WithSettings() {
    "CreateAttributes" should "create the correct list of attributes for a complex Json" in {
      val sparkImplicits = sparkSession.implicits
      import sparkImplicits._

      val ComplexjsonStr = """{ "metadata": { "key": 84896, "value": 54, "values": [1, 2, 3] }}"""

      val df = sparkSession.read
        .option("inferSchema", value = true)
        .json(Seq(ComplexjsonStr).toDS())

      val complexAttr2 = TableAttribute("key", "long", Some(false), required = None)
      val complexAttr3 =
        TableAttribute("value", "long", Some(false), required = None)
      val complexAttr4 = TableAttribute("values", "long", Some(true), required = None)

      val complexAttr1: List[TableAttribute] = List(
        TableAttribute(
          "metadata",
          "struct",
          Some(false),
          required = None,
          attributes = List(complexAttr2, complexAttr3, complexAttr4)
        )
      )

      val complexAttr: List[TableAttribute] =
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

      val simpleAttr: List[TableAttribute] =
        InferSchemaHandler.createAttributes(Map.empty, df1.schema)

      val simpleAttr1: List[TableAttribute] = List(
        TableAttribute("key", "string", Some(false), required = None),
        TableAttribute("value", "long", Some(false), required = None)
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

      val arrayAttr: List[TableAttribute] =
        InferSchemaHandler.createAttributes(Map.empty, df1.schema)

      val arrayAttr1: List[TableAttribute] = List(
        TableAttribute("id", "long", Some(false), required = None),
        TableAttribute("name", "string", Some(true), required = None)
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

      val dsv: List[TableAttribute] =
        InferSchemaHandler.createAttributes(Map.empty, df1.schema, forcePattern = false)

      val dsv1: List[TableAttribute] = List(
        TableAttribute(
          "first name",
          "string",
          Some(false),
          required = None,
          rename = Some("first_name")
        ),
        TableAttribute(
          "last name",
          "string",
          Some(false),
          required = None,
          rename = Some("last_name")
        ),
        TableAttribute("age", "string", Some(false), required = None),
        TableAttribute("ok", "string", Some(false), required = None)
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

      val dsv: List[TableAttribute] = InferSchemaHandler.createAttributes(Map.empty, df1.schema)

      val dsv1: List[TableAttribute] = List(
        TableAttribute("_c0", "string", Some(false), required = None, rename = Some("c0")),
        TableAttribute("_c1", "string", Some(false), required = None, rename = Some("c1")),
        TableAttribute("_c2", "int", Some(false), required = None, rename = Some("c2"))
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
