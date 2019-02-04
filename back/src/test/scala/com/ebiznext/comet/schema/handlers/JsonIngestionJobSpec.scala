package com.ebiznext.comet.schema.handlers

import java.io.InputStream

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.sample.SampleData
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.scalatest.{FlatSpec, Matchers}

class JsonIngestionJobSpec extends FlatSpec with Matchers with SampleData {
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

    val res1 = JsonIngestionUtil.parseString(json)
    val res2 = JsonIngestionUtil.parseString(json)
    println(res1.toString)
    val res = for {
      t1 <- res1
      t2 <- res2

    } yield {
      JsonIngestionUtil.compareTypes(Nil, ("root", t1, true), ("root", t2, true))
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

    val res1 = JsonIngestionUtil.parseString(json1)
    val res2 = JsonIngestionUtil.parseString(json2)
    println(res1)
    println(res2)
    println(res1.toString)
    val res = for {
      t1 <- res1
      t2 <- res2

    } yield {
      JsonIngestionUtil.compareTypes(Nil, ("root", t1, true), ("root", t2, true))
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
        |           "myArray":[1, 2]
        |}
      """.stripMargin

    val json2 =
      """
        |{
        |           "abc": {
        |           "x":"y"
        |           },
        |						"GlossSeeAlso": ["GML", null],
        |           "myArray":[1, 2.2],
        |           "unknown":null
        |}
      """.stripMargin

    val res1 = JsonIngestionUtil.parseString(json1)
    val res2 = JsonIngestionUtil.parseString(json2)
    println(res1)
    println(res2)
    println(res1.toString)
    val res = for {
      t1 <- res1
      t2 <- res2

    } yield {
      JsonIngestionUtil.compareTypes(Nil, ("root", t1, true), ("root", t2, true))
    }
    println(res)
  }

  "Ingest Complex JSON" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "json.yml")
    sh.write(loadFile("/sample/json/json.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/json/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream = getClass.getResourceAsStream("/sample/json/complex.json")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath = DatasetArea.path(DatasetArea.pending("json"), "complex.json")
    storageHandler.write(lines, targetPath)
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }
}
