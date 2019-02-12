package com.ebiznext.comet.schema.handlers

import java.io.InputStream
import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.apache.spark.sql.types._

import scala.util.Success

class JsonIngestionJobSpec extends TestHelper {
  // TODO Change naming
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
  }
  // TODO Change naming
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
  }
  // TODO Change naming
  "Parse compatible json" should "fail" in {
    val json1 =
      """
        |{
        |   "complexArray": [ {"a": "Hello"}, {"a": "Hello"} ],
        |	  "GlossSeeAlso": ["GML", "XML"],
        |   "myArray":[1, 2]
        |}
      """.stripMargin

    val json2 =
      """
        |{
        |  "abc": { "x":"y" },
        |	 "GlossSeeAlso": ["GML", null],
        |  "myArray":[1, 2.2],
        |  "unknown":null
        |}
      """.stripMargin

    JsonIngestionUtil.parseString(json1) shouldBe Success(
      StructType(
        Seq(
          StructField(
            "complexArray",
            ArrayType(StructType(Seq(StructField("a", StringType, true))), true),
            true
          ),
          StructField("GlossSeeAlso", ArrayType(StringType, true), true),
          StructField("myArray", ArrayType(LongType, true), true)
        )
      )
    )

    JsonIngestionUtil.parseString(json2) shouldBe Success(
      StructType(
        Seq(
          StructField("abc", StructType(Seq(StructField("x", StringType, true))), true),
          StructField("GlossSeeAlso", ArrayType(StringType, true), true),
          StructField("myArray", ArrayType(DoubleType, true), true),
          StructField("unknown", NullType, true)
        )
      )
    )

  }

  // TODO Fix warning :) And should we test sth ?
  "Ingest Complex JSON" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "json.yml")
    sh.write(loadFile("/sample/json/json.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/json/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream =
      getClass.getResourceAsStream("/sample/json/complex.json")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath =
      DatasetArea.path(DatasetArea.pending("json"), "complex.json")
    storageHandler.write(lines, targetPath)
    val validator =
      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
    //TODO Complete test
  }
}
