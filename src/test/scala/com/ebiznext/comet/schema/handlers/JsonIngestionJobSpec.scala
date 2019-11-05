/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
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

    new SpecTrait {
      cleanMetadata
      cleanDatasets
      override val domainFilename: String = "json.yml"
      override val sourceDomainPathname: String = "/sample/json/json.yml"

      override val datasetDomainName: String = "json"
      override val sourceDatasetPathName: String = "/sample/json/complex.json"

      loadPending

      // Check archive

      readFileContent(cometDatasetsPath + s"/archive/${datasetDomainName}/complex.json") shouldBe loadFile(
        "/sample/json/complex.json"
      )

      // Accepted should have the same data as input
      sparkSession.read
        .parquet(
          cometDatasetsPath + s"/accepted/${datasetDomainName}/sample_json/${getTodayPartitionPath}"
        )
        .except(
          sparkSession.read
            .json(getClass.getResource(s"/sample/${datasetDomainName}/complex.json").toURI.getPath)
        )
        .count() shouldBe 0
    }

  }
}
