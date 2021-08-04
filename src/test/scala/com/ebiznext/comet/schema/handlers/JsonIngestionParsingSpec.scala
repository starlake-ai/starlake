package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.apache.spark.sql.types._

import scala.util.Success

class JsonIngestionParsingSpec extends TestHelper {

  "Parse valid json" should "succeed" in {
    val json =
      """
        |{
        |    "glossary": {
        |        "title": "example glossary",
        |		"GlossDiv": {
        |            "title": "S",
        |			"GlossList": {
        |                "GlossEntry": {
        |                    "ID": "SGML",
        |					"SortAs": "SGML",
        |					"GlossTerm": "Standard Generalized Markup Language",
        |					"Acronym": "SGML",
        |					"Abbrev": "ISO 8879:1986",
        |					"GlossDef": {
        |                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
        |						"GlossSeeAlso": ["GML", "XML"],
        |           "IntArray":[1, 2]
        |                    },
        |					"GlossSee": "markup"
        |                }
        |            }
        |        }
        |    }
        |}
      """.stripMargin

    JsonIngestionUtil.parseString(json) shouldBe Success(
      StructType(
        Seq(
          StructField(
            "glossary",
            StructType(
              Seq(
                StructField("title", StringType, true),
                StructField(
                  "GlossDiv",
                  StructType(
                    Seq(
                      StructField("title", StringType, true),
                      StructField(
                        "GlossList",
                        StructType(
                          Seq(
                            StructField(
                              "GlossEntry",
                              StructType(
                                Seq(
                                  StructField("ID", StringType, true),
                                  StructField("SortAs", StringType, true),
                                  StructField("GlossTerm", StringType, true),
                                  StructField("Acronym", StringType, true),
                                  StructField("Abbrev", StringType, true),
                                  StructField(
                                    "GlossDef",
                                    StructType(
                                      Seq(
                                        StructField("para", StringType, true),
                                        StructField(
                                          "GlossSeeAlso",
                                          ArrayType(StringType, true),
                                          true
                                        ),
                                        StructField("IntArray", ArrayType(LongType, true), true)
                                      )
                                    ),
                                    true
                                  ),
                                  StructField("GlossSee", StringType, true)
                                )
                              ),
                              true
                            )
                          )
                        ),
                        true
                      )
                    )
                  ),
                  true
                )
              )
            ),
            true
          )
        )
      )
    )

    val json1 =
      """
        |{
        |						"GlossSeeAlso": ["GML", "XML"],
        |           "IntArray":[1.1, 2.2]
        |}
      """.stripMargin

    val json2 =
      """
        |{
        |						"GlossSeeAlso": ["GML", null],
        |           "IntArray":[1, 2]
        |}
      """.stripMargin

    JsonIngestionUtil.parseString(json1) shouldBe Success(
      StructType(
        Seq(
          StructField("GlossSeeAlso", ArrayType(StringType, true), true),
          StructField("IntArray", ArrayType(DoubleType, true), true)
        )
      )
    )

    JsonIngestionUtil.parseString(json2) shouldBe Success(
      StructType(
        Seq(
          StructField("GlossSeeAlso", ArrayType(StringType, true), true),
          StructField("IntArray", ArrayType(LongType, true), true)
        )
      )
    )

    val json3 =
      """[
        |  {
        |    "st": "id1",
        |    "meI": "value",
        |    "meN": "value",
        |    "ma": true
        |  },
        |  {
        |    "st": "id2",
        |    "a": "id3",
        |    "se": "value",
        |    "meI": "value",
        |    "meN": "value"
        |  }
        |]""".stripMargin

    JsonIngestionUtil.parseString(json3) shouldBe Success(
      ArrayType(
        StructType(
          Seq(
            StructField("a", StringType, nullable = true),
            StructField("ma", BooleanType, nullable = true),
            StructField("meI", StringType, nullable = true),
            StructField("meN", StringType, nullable = true),
            StructField("se", StringType, nullable = true),
            StructField("st", StringType, nullable = true)
          )
        ),
        containsNull = true
      )
    )

  }

  "Parse invalid json" should "fail" in {
    val json1 =
      """
        |{
        |   "complexArray": [ {"a": "Hello"}, {"a": "Hello"} ],
        |	  "GlossSeeAlso": ["GML", "XML"]
        |   "myArray":[1, 2]
        |}
      """.stripMargin

    JsonIngestionUtil.parseString(json1).isSuccess shouldBe false
  }
}
