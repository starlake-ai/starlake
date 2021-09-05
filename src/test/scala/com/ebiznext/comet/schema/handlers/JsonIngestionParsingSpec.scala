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
                StructField("title", StringType, nullable = true),
                StructField(
                  "GlossDiv",
                  StructType(
                    Seq(
                      StructField("title", StringType, nullable = true),
                      StructField(
                        "GlossList",
                        StructType(
                          Seq(
                            StructField(
                              "GlossEntry",
                              StructType(
                                Seq(
                                  StructField("ID", StringType, nullable = true),
                                  StructField("SortAs", StringType, nullable = true),
                                  StructField("GlossTerm", StringType, nullable = true),
                                  StructField("Acronym", StringType, nullable = true),
                                  StructField("Abbrev", StringType, nullable = true),
                                  StructField(
                                    "GlossDef",
                                    StructType(
                                      Seq(
                                        StructField("para", StringType, nullable = true),
                                        StructField(
                                          "GlossSeeAlso",
                                          ArrayType(StringType, containsNull = true),
                                          nullable = true
                                        ),
                                        StructField("IntArray", ArrayType(LongType, containsNull = true), nullable = true)
                                      )
                                    ),
                                    nullable = true
                                  ),
                                  StructField("GlossSee", StringType, nullable = true)
                                )
                              ),
                              nullable = true
                            )
                          )
                        ),
                        nullable = true
                      )
                    )
                  ),
                  nullable = true
                )
              )
            ),
            nullable = true
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
          StructField("GlossSeeAlso", ArrayType(StringType, containsNull = true), nullable = true),
          StructField("IntArray", ArrayType(DoubleType, containsNull = true), nullable = true)
        )
      )
    )

    JsonIngestionUtil.parseString(json2) shouldBe Success(
      StructType(
        Seq(
          StructField("GlossSeeAlso", ArrayType(StringType, containsNull = true), nullable = true),
          StructField("IntArray", ArrayType(LongType, containsNull = true), nullable = true)
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
