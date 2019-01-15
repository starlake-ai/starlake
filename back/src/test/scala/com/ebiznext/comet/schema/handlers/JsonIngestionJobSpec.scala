package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.job.JsonIngestionJob
import org.apache.spark.sql.execution.datasources.json.JsonUtil
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try

class JsonIngestionJobSpec extends FlatSpec with Matchers {
  "Parse exact same json" should "succeed" in {
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

    val res1 = JsonUtil.parseString(json)
    val res2 = JsonUtil.parseString(json)
    println(res1.toString)
    val res = for {
      t1 <- res1
      t2 <- res2

    } yield {
      JsonUtil.compareTypes(Nil, ("root", t1, true), ("root", t2, true))
    }
    println(res)
  }

  "Parse compatible json" should "succeed" in {
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
        |           "IntArray":[1, 2],
        |}
      """.stripMargin

    val res1 = JsonUtil.parseString(json1)
    val res2 = JsonUtil.parseString(json2)
    println(res1)
    println(res2)
    println(res1.toString)
    val res = for {
      t1 <- res1
      t2 <- res2

    } yield {
      JsonUtil.compareTypes(Nil, ("root", t1, true), ("root", t2, true))
    }
    println(res)
  }

  "Parse compatible json" should "fail" in {
    val json1 =
      """
        |{
        |           "complexArray": [
        |              {"a": "Hello"},
        |              {"a": "Hello"}
        |                   ],
        |						"GlossSeeAlso": ["GML", "XML"],
        |           "IntArray":[1, 2]
        |}
      """.stripMargin

    val json2 =
      """
        |{
        |           "abc": {
        |           "x":"y"
        |           },
        |						"GlossSeeAlso": ["GML", null],
        |           "IntArray":[1, 2.2],
        |           "unknown":null
        |}
      """.stripMargin

    val res1 = JsonUtil.parseString(json1)
    val res2 = JsonUtil.parseString(json2)
    println(res1)
    println(res2)
    println(res1.toString)
    val res = for {
      t1 <- res1
      t2 <- res2

    } yield {
      JsonUtil.compareTypes(Nil, ("root", t1, true), ("root", t2, true))
    }
    println(res)
  }

}
