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
import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.model._
import com.softwaremill.sttp.{HttpURLConnectionBackend, _}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata => _, _}

import scala.util.Try
import com.databricks.spark.xml._

class SchemaHandlerSpec extends TestHelper {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    es.start()

  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    es.stop()
  }

  private val playerSchema: StructType = StructType(
    Seq(
      StructField("PK", StringType),
      StructField("firstName", StringType),
      StructField("lastName", StringType),
      StructField("DOB", DateType),
      StructField("YEAR", IntegerType),
      StructField("MONTH", IntegerType)
    )
  )

  new WithSettings() {
    // TODO Helper (to delete)
    "Ingest CSV" should "produce file in accepted" in {

      new SpecTrait(
        domainFilename = "DOMAIN.comet.yml",
        sourceDomainPathname = s"/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {

        cleanMetadata
        cleanDatasets

        loadPending

        // Check Archived
        readFileContent(
          cometDatasetsPath + s"/archive/$datasetDomainName/SCHEMA-VALID.dsv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Check rejected

        val rejectedDf = sparkSession.read
          .parquet(cometDatasetsPath + s"/rejected/$datasetDomainName/User")

        val expectedRejectedF = prepareDateColumns(
          sparkSession.read
            .schema(prepareSchema(rejectedDf.schema))
            .json(getResPath("/expected/datasets/rejected/DOMAIN.json"))
        )

        expectedRejectedF.except(rejectedDf).count() shouldBe 1

        // Accepted should have the same data as input
        val acceptedDf = sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/User/$getTodayPartitionPath")

        printDF(acceptedDf, "acceptedDf")
        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))

        printDF(expectedAccepted, "expectedAccepted")
        acceptedDf
          .select("firstname")
          .except(expectedAccepted.select("firstname"))
          .count() shouldBe 0

        if (settings.comet.isElasticsearchSupported()) {
          implicit val backend = HttpURLConnectionBackend()
          val countUri = uri"http://127.0.0.1:9200/domain.user/_count"
          val response = sttp.get(countUri).send()
          response.code should be <= 299
          response.code should be >= 200
          assert(response.body.isRight)
          response.body.right.toString() contains "\"count\":2"
        }
      }
    }

    "Ingest schema with partition" should "produce partitioned output in accepted" in {
      new SpecTrait(
        domainFilename = "DOMAIN.comet.yml",
        sourceDomainPathname = s"/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending

        private val firstLevel: List[Path] = storageHandler.listDirectories(
          new Path(cometDatasetsPath + s"/accepted/$datasetDomainName/Players")
        )

        firstLevel.size shouldBe 2
        firstLevel.foreach(storageHandler.listDirectories(_).size shouldBe 2)

        sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/Players")
          .except(
            sparkSession.read
              .option("header", "false")
              .schema(playerSchema)
              .csv(getResPath("/sample/Players.csv"))
          )
          .count() shouldBe 0

      }
    }

    "Ingest schema with merge" should "produce merged results accepted" in {
      new SpecTrait(
        domainFilename = "DOMAIN.comet.yml",
        sourceDomainPathname = s"/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
      }

      new SpecTrait(
        domainFilename = "DOMAIN.comet.yml",
        sourceDomainPathname = s"/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players-merge.csv"
      ) {

        loadPending

        val acceptedDf: DataFrame = sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/Players")

        val players: DataFrame = sparkSession.read
          .option("header", "false")
          .option("encoding", "UTF-8")
          .schema(playerSchema)
          .csv(getResPath("/sample/Players.csv"))

        val playersMerge: DataFrame = sparkSession.read
          .option("header", "false")
          .option("encoding", "UTF-8")
          .schema(playerSchema)
          .csv(getResPath("/sample/Players-merge.csv"))

        val playersPk: Array[String] = players.select("PK").collect().map(_.getString(0))

        val expected: DataFrame = playersMerge
          .union(players.join(playersMerge, Seq("PK"), "left_anti"))
          .union(players.filter(!col("PK").isin(playersPk: _*)))

        acceptedDf
          .except(expected)
          .count() shouldBe 0

      }
    }

    "Ingest Dream Contact CSV" should "produce file in accepted" in {
      new SpecTrait(
        domainFilename = "dream.comet.yml",
        sourceDomainPathname = s"/sample/dream/dream.comet.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Contact_20190101_090800_008.psv"
      ) {

        cleanMetadata

        cleanDatasets

        loadPending

        readFileContent(
          cometDatasetsPath + s"/archive/$datasetDomainName/OneClient_Contact_20190101_090800_008.psv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
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
          .parquet(
            cometDatasetsPath + s"/accepted/$datasetDomainName/client/${getTodayPartitionPath}"
          )
          // Timezone Problem
          .drop("customer_creation_date")

        acceptedDf.show()

        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/dream/client.json"))
            // Timezone Problem
            .drop("customer_creation_date")
            .withColumn("truncated_zip_code", substring(col("zip_code"), 0, 3))
            .withColumn("source_file_name", lit("OneClient_Contact_20190101_090800_008.psv"))

        expectedAccepted.show()
        acceptedDf.except(expectedAccepted).count() shouldBe 0
      }

    }

    "Ingest Dream Segment CSV" should "produce file in accepted" in {

      new SpecTrait(
        domainFilename = "dream.comet.yml",
        sourceDomainPathname = "/sample/dream/dream.comet.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        cleanMetadata
        cleanDatasets

        loadPending

        readFileContent(
          cometDatasetsPath + s"/archive/$datasetDomainName/OneClient_Segmentation_20190101_090800_008.psv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Accepted should have the same data as input
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/$datasetDomainName/segment/${getTodayPartitionPath}"
          )

        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/dream/segment.json"))

        acceptedDf.except(expectedAccepted).count() shouldBe 0

      }

    }

    "Ingest Locations JSON" should "produce file in accepted" in {

      new SpecTrait(
        domainFilename = "locations.comet.yml",
        sourceDomainPathname = s"/sample/simple-json-locations/locations.comet.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        cleanDatasets

        loadPending

        readFileContent(
          cometDatasetsPath + s"/${settings.comet.area.archive}/$datasetDomainName/locations.json"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Accepted should have the same data as input
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/$datasetDomainName/locations/$getTodayPartitionPath"
          )

        val expectedAccepted =
          sparkSession.read
            .json(
              getResPath("/expected/datasets/accepted/locations/locations.json")
            )
            .withColumn("name_upper_case", upper(col("name")))
            .withColumn("source_file_name", lit("locations.json"))

        acceptedDf
          .except(expectedAccepted.select(acceptedDf.columns.map(col): _*))
          .count() shouldBe 0

      }

    }
    "Ingest Locations XML" should "produce file in accepted" in {

      new SpecTrait(
        domainFilename = "locations.comet.yml",
        sourceDomainPathname = s"/sample/xml/locations.comet.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/xml/locations.xml"
      ) {
        cleanMetadata
        cleanDatasets

        loadPending

        readFileContent(
          cometDatasetsPath + s"/${settings.comet.area.archive}/$datasetDomainName/locations.xml"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Accepted should have the same data as input
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/$datasetDomainName/locations/$getTodayPartitionPath"
          )

        val expectedAccepted =
          sparkSession.read
            .option("rowTag", "element")
            .xml(
              getResPath("/expected/datasets/accepted/locations/locations.xml")
            )

        acceptedDf.except(expectedAccepted).count() shouldBe 0

      }

    }
    "Load Business with Transform Tag" should "load an AutoDesc" in {
      new SpecTrait(
        domainFilename = "locations.comet.yml",
        sourceDomainPathname = "/sample/simple-json-locations/locations.comet.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        import org.scalatest.TryValues._
        cleanMetadata
        cleanDatasets
        val schemaHandler = new SchemaHandler(storageHandler)
        val filename = "/sample/metadata/business/business.comet.yml"
        val jobPath = new Path(getClass.getResource(filename).toURI)
        val job = schemaHandler.loadJobFromFile(jobPath)
        job.success.value.name shouldBe "business1"
      }
    }

    //TODO TOFIX
    //  "Load Business Definition" should "produce business dataset" in {
    //    val sh = new HdfsStorageHandler
    //    val jobsPath = new Path(DatasetArea.jobs, "sample/metadata/business/business.comet.yml")
    //    sh.write(loadFile("/sample/metadata/business/business.comet.yml"), jobsPath)
    //    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
    //    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    //    validator.autoJob("business1")
    //  }

    "Writing types" should "work" in {

      val typesPath = new Path(DatasetArea.types, "types.comet.yml")

      deliverTestFile("/sample/types.comet.yml", typesPath)

      readFileContent(typesPath) shouldBe loadTextFile("/sample/types.comet.yml")
    }

    "Mapping Schema" should "produce valid template" in {
      new SpecTrait(
        domainFilename = "locations.comet.yml",
        sourceDomainPathname = "/sample/simple-json-locations/locations.comet.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        cleanDatasets

        val schemaHandler = new SchemaHandler(storageHandler)

        val schema: Option[Schema] = schemaHandler.domains
          .find(_.name == "locations")
          .flatMap(_.schemas.find(_.name == "locations"))
        val expected: String =
          """
            |{
            |  "index_patterns": ["locations.locations", "locations.locations-*"],
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
            |},
            |"name_upper_case": {
            |  "type": "keyword"
            |},
            |"source_file_name": {
            |  "type": "keyword"
            |}
            |}
            |    }
            |  }
            |}
        """.stripMargin.trim
        val mapping =
          schema.map(_.mapping(None, "locations", schemaHandler)).map(_.trim).getOrElse("")
        logger.info(mapping)
        mapping shouldBe expected
      }

    }
    "JSON Schema" should "produce valid template" in {
      new SpecTrait(
        domainFilename = "locations.comet.yml",
        sourceDomainPathname = s"/sample/simple-json-locations/locations.comet.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        cleanDatasets

        val schemaHandler = new SchemaHandler(storageHandler)

        val ds: URL = getClass.getResource("/sample/mapping/dataset")

        logger.info(
          Schema.mapping(
            "domain",
            "schema",
            StructField("ignore", sparkSession.read.parquet(ds.toString).schema),
            schemaHandler
          )
        )

        // TODO: we aren't actually testing anything here are we?
      }
    }

    "Custom mapping in Metadata" should "be read as a map" in {
      val sch = new SchemaHandler(storageHandler)
      val content =
        """mode: FILE
          |withHeader: false
          |encoding: ISO-8859-1
          |format: POSITION
          |sink:
          |  type: BQ
          |  timestamp: _PARTITIONTIME
          |write: OVERWRITE
          |""".stripMargin
      val metadata = sch.mapper.readValue(content, classOf[Metadata])

      metadata shouldBe Metadata(
        mode = Some(Mode.FILE),
        format = Some(com.ebiznext.comet.schema.model.Format.POSITION),
        encoding = Some("ISO-8859-1"),
        withHeader = Some(false),
        sink = Some(BigQuerySink(timestamp = Some("_PARTITIONTIME"))),
        write = Some(WriteMode.OVERWRITE)
      )
    }
  }
}
