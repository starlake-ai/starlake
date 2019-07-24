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

import java.net.URL

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.model.{Attribute, Schema}
import com.ebiznext.comet.{TestHelper, TypeToImport}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

import scala.util.Try

class SchemaHandlerSpec extends TestHelper {

  // TODO Helper (to delete)
  "Ingest CSV" should "produce file in accepted" in {

    new SpecTrait {
      override val domainName: String = "DOMAIN.yml"
      override val domainFile: String = s"/sample/$domainName"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "types.yml",
          "/sample/types.yml"
        )
      )

      override val schemaName: String = "DOMAIN"
      override val dataset: String = "/sample/SCHEMA-VALID.dsv"

      loadPending

      // Check Archived
      readFileContent(cometDatasetsPath + s"/archive/$schemaName/SCHEMA-VALID.dsv") shouldBe loadFile(
        dataset
      )

      // Check rejected

      val rejectedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/rejected/$schemaName/User")

      val expectedRejectedF = prepareDateColumns(
        sparkSession.read
          .schema(prepareSchema(rejectedDf.schema))
          .json(getResPath("/expected/datasets/rejected/DOMAIN.json"))
      )

      expectedRejectedF.except(rejectedDf).count() shouldBe 0

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$schemaName/User/$getTodayPartitionPath")

      printDF(acceptedDf, "acceptedDf")
      val expectedAccepted =
        sparkSession.read
          .schema(acceptedDf.schema)
          .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))

      printDF(expectedAccepted, "expectedAccepted")
      acceptedDf.select("firstname").except(expectedAccepted.select("firstname")).count() shouldBe 0

    }
  }

  "Ingest Dream Contact CSV" should "produce file in accepted" in {
    new SpecTrait {
      override val domainName: String = "dream.yml"
      override val domainFile: String = s"/sample/dream/$domainName"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "default.yml",
          "/sample/default.yml"
        ),
        TypeToImport(
          "types.yml",
          "/sample/dream/types.yml"
        )
      )

      override val schemaName: String = "dream"
      override val dataset: String = "/sample/dream/OneClient_Contact_20190101_090800_008.psv"

      loadPending

      readFileContent(
        cometDatasetsPath + s"/archive/$schemaName/OneClient_Contact_20190101_090800_008.psv"
      ) shouldBe loadFile(
        dataset
      )

      //If we run this test alone, we do not have rejected, else we have rejected but not accepted ...
      Try {
        printDF(
          sparkSession.read.parquet(
            cometDatasetsPath + "/rejected/dream/client"
          ),
          "dream/client"
        )
      }

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$schemaName/client/${getTodayPartitionPath}")

      val expectedAccepted =
        sparkSession.read
          .schema(acceptedDf.schema)
          .json(getResPath("/expected/datasets/accepted/dream/client.json"))

      expectedAccepted.show(false)
      acceptedDf.show(false)
      acceptedDf.select("dream_id").except(expectedAccepted.select("dream_id")).count() shouldBe 0
    }

  }

  "Ingest Dream Segment CSV" should "produce file in accepted" in {

    new SpecTrait {
      override val domainName: String = "dream.yml"
      override val domainFile: String = s"/sample/dream/$domainName"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "default.yml",
          "/sample/default.yml"
        ),
        TypeToImport(
          "dream.yml",
          "/sample/dream/types.yml"
        )
      )

      override val schemaName: String = "dream"
      override val dataset: String =
        "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"

      loadPending

      readFileContent(
        cometDatasetsPath + s"/archive/$schemaName/OneClient_Segmentation_20190101_090800_008.psv"
      ) shouldBe loadFile(
        dataset
      )

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$schemaName/segment/${getTodayPartitionPath}")

      val expectedAccepted =
        sparkSession.read
          .schema(acceptedDf.schema)
          .json(getResPath("/expected/datasets/accepted/dream/segment.json"))

      acceptedDf.except(expectedAccepted).count() shouldBe 0

    }

  }

  "Ingest Dream Locations JSON" should "produce file in accepted" in {

    new SpecTrait {
      override val domainName: String = "locations.yml"
      override val domainFile: String = s"/sample/simple-json-locations/$domainName"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "types.yml",
          "/sample/simple-json-locations/types.yml"
        )
      )

      override val schemaName: String = "locations"
      override val dataset: String =
        "/sample/simple-json-locations/locations.json"

      loadPending

      readFileContent(
        cometDatasetsPath + s"/archive/$schemaName/locations.json"
      ) shouldBe loadFile(
        dataset
      )

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$schemaName/locations/$getTodayPartitionPath")

      val expectedAccepted =
        sparkSession.read
          .json(
            getResPath("/expected/datasets/accepted/locations/locations.json")
          )

      acceptedDf.except(expectedAccepted).count() shouldBe 0

    }

  }
  //TODO TOFIX
  //  "Load Business Definition" should "produce business dataset" in {
  //    val sh = new HdfsStorageHandler
  //    val jobsPath = new Path(DatasetArea.jobs, "sample/metadata/business/business.yml")
  //    sh.write(loadFile("/sample/metadata/business/business.yml"), jobsPath)
  //    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
  //    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
  //    validator.autoJob("business1")
  //  }

  "Writing types" should "work" in {

    val typesPath = new Path(DatasetArea.types, "types.yml")

    storageHandler.write(loadFile("/sample/types.yml"), typesPath)

    readFileContent(typesPath) shouldBe loadFile("/sample/types.yml")

  }

  "Mapping Schema" should "produce valid template" in {
    new SpecTrait {
      override val domainName: String = "locations.yml"
      override val domainFile: String = s"/sample/simple-json-locations/$domainName"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "types.yml",
          "/sample/simple-json-locations/types.yml"
        )
      )

      override val schemaName: String = "locations"
      override val dataset: String =
        "/sample/simple-json-locations/locations.json"

      init()
      val schema: Option[Schema] = schemaHandler.domains
        .find(_.name == "locations")
        .flatMap(_.schemas.find(_.name == "locations"))
      val expected: String =
        """
          |{
          |  "index_patterns": ["locations_locations", "locations_locations-*"],
          |  "settings": {
          |    "number_of_shards": "1",
          |    "number_of_replicas": "0"
          |  },
          |  "mappings": {
          |    "_doc": {
          |      "_source": {
          |        "enabled": true
          |      },
          |
          |"properties": {
          |
          |"id": {
          |  "type": "keyword"
          |},
          |"name": {
          |  "type": "keyword"
          |}
          |}
          |    }
          |  }
          |}
        """.stripMargin.trim
      val mapping = schema.map(_.mapping(None, "locations")).map(_.trim).getOrElse("")
      println(mapping)
      mapping shouldBe expected
    }

  }
  "JSON Schema" should "produce valid template" in {
    new SpecTrait {
      override val domainName: String = "locations.yml"
      override val domainFile: String = s"/sample/simple-json-locations/$domainName"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "types.yml",
          "/sample/mapping/types.yml"
        )
      )

      override val schemaName: String = "locations"
      override val dataset: String =
        "/sample/simple-json-locations/locations.json"

      init()

      val ds: URL = getClass.getResource("/sample/mapping/dataset")

      println(
        Schema.mapping(
          "domain",
          "schema",
          StructField("ignore", sparkSession.read.parquet(ds.toString).schema)
        )
      )
    }
  }

}
