package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.apache.spark.sql.types._
import scala.util.Success

class JsonIngestionJobSpec extends TestHelper {

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

  "Ingest Complex JSON" should "should be ingested from pending to accepted, and archived" in {

    val domainsPath = new Path(DatasetArea.domains, "json.yml")
    storageHandler.write(loadFile("/sample/json/json.yml"), domainsPath)

    val typesPath = new Path(DatasetArea.types, "types.yml")
    storageHandler.write(loadFile("/sample/json/types.yml"), typesPath)

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val targetPath =
      DatasetArea.path(DatasetArea.pending("json"), "complex.json")

    storageHandler.write(loadFile("/sample/json/complex.json"), targetPath)

    val validator =
      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()

    // Check archive

    readFileContent(cometDatasetsPath + "/archive/json/complex.json") shouldBe loadFile(
      "/sample/json/complex.json"
    )

    // Accepted should have the same data as input
    sparkSession.read
      .parquet(cometDatasetsPath + s"/accepted/json/sample_json/${getTodayPartitionPath}")
      .except(
        sparkSession.read.json(getClass.getResource("/sample/json/complex.json").toURI.getPath)
      )
      .count() shouldBe 0

  }
}
