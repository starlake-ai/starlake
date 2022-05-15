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

package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.schema.generator.Yml2GraphViz
import ai.starlake.schema.model._
import better.files.File
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata => _, _}
import org.apache.spark.sql.{DataFrame, Row}

import java.net.URL
import scala.util.Try

class SchemaHandlerSpec extends TestHelper {

  override def afterAll(): Unit = {
    // We need to start it manually because we need to access the HTTP mapped port
    // in the configuration below before any test get executed.
    esContainer.stop()
    super.afterAll()
  }

  private val playerSchema = StructType(
    Seq(
      StructField("PK", StringType),
      StructField("firstName", StringType),
      StructField("lastName", StringType),
      StructField("DOB", DateType),
      StructField("YEAR", IntegerType),
      StructField("MONTH", IntegerType)
    )
  )

  val configuration: Config =
    ConfigFactory
      .parseString(s"""
                     |elasticsearch {
                     |  active = true
                     |  options = {
                     |    "es.nodes.wan.only": "true"
                     |    "es.nodes": "localhost"
                     |    "es.port": "${esContainer.httpHostAddress.substring(
                       esContainer.httpHostAddress.lastIndexOf(':') + 1
                     )}",
                     |
                     |    #  net.http.auth.user = ""
                     |    #  net.http.auth.pass = ""
                     |
                     |    "es.net.ssl": "false"
                     |    "es.net.ssl.cert.allow.self.signed": "false"
                     |
                     |    "es.batch.size.entries": "1000"
                     |    "es.batch.size.bytes": "1mb"
                     |    "es.batch.write.retry.count": "3"
                     |    "es.batch.write.retry.wait": "10s"
                     |  }
                     |}
                     |""".stripMargin)
      .withFallback(super.testConfiguration)

  new WithSettings(configuration) {
    // TODO Helper (to delete)
    "Ingest CSV" should "produce file in accepted" in {

      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = s"/sample/DOMAIN.comet.yml",
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

        val expectedRejectedF =
          sparkSession.read
            .schema(prepareSchema(rejectedDf.schema))
            .json(getResPath("/expected/datasets/rejected/DOMAIN.json"))

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

        val countUri = s"http://${esContainer.httpHostAddress}/domain.user/_count"
        val getRequest = new HttpGet(countUri)
        getRequest.setHeader("Content-Type", "application/json")
        val client = HttpClients.createDefault
        val response = client.execute(getRequest)

        response.getStatusLine.getStatusCode should be <= 299
        response.getStatusLine.getStatusCode should be >= 200
        EntityUtils.toString(response.getEntity()) contains "\"count\":2"

      }
    }
    "Ingest empty file with DSV schema" should "be ok " in {
      new WithSettings {
        new SpecTrait(
          domainOrJobFilename = "DOMAIN.comet.yml",
          sourceDomainOrJobPathname = s"/sample/DOMAIN.comet.yml",
          datasetDomainName = "DOMAIN",
          sourceDatasetPathName = "/sample/employee-empty.csv"
        ) {
          cleanMetadata
          cleanDatasets
          loadPending shouldBe true
        }
      }
    }

    "load File" should "work" in {
      new WithSettings {
        new SpecTrait(
          domainOrJobFilename = "DOMAIN.comet.yml",
          sourceDomainOrJobPathname = s"/sample/DOMAIN.comet.yml",
          datasetDomainName = "DOMAIN",
          sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
        ) {
          val targetPath = DatasetArea.path(
            DatasetArea.pending("DOMAIN.comet.yml"),
            new Path("/sample/SCHEMA-VALID.dsv").getName
          )
          cleanMetadata
          cleanDatasets
          load(LoadConfig(domainOrJobFilename, "User", List(targetPath))) shouldBe true
        }
      }
    }

    "load to elasticsearch" should "work" in {
      new WithSettings(configuration) {
        new SpecTrait(
          domainOrJobFilename = "locations.comet.yml",
          sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.comet.yml",
          datasetDomainName = "locations",
          sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
        ) {
          cleanMetadata
          cleanDatasets
          // loadPending
          val validator = loadWorkflow()
          val result = validator.esLoad(
            ESLoadConfig(
              domain = "DOMAIN",
              schema = "",
              format = "json",
              dataset = Some(
                Left(new Path(cometDatasetsPath + s"/pending/$datasetDomainName/locations.json"))
              )
            )
          )
          result.isSuccess shouldBe true
        }
      }
    }

    "Ingest schema with partition" should "produce partitioned output in accepted" in {
      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = s"/sample/DOMAIN.comet.yml",
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
        domainOrJobFilename = "simple-merge.comet.yml",
        sourceDomainOrJobPathname = s"/sample/merge/simple-merge.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
      }

      new SpecTrait(
        domainOrJobFilename = "merge-with-timestamp.comet.yml",
        sourceDomainOrJobPathname = s"/sample/merge/merge-with-timestamp.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players-merge.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending

        val accepted: Array[Row] = sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/Players")
          .collect()

        // Input contains a row with an older timestamp
        // With MergeOptions.timestamp set, that row should be ignored (the rest should be updated)

        val expected: Array[Row] =
          sparkSession.read
            .option("header", "false")
            .option("encoding", "UTF-8")
            .schema(playerSchema)
            .csv(getResPath("/expected/datasets/accepted/DOMAIN/Players-merged-with-timestamp.csv"))
            .collect()

        accepted should contain theSameElementsAs expected
      }

      new SpecTrait(
        domainOrJobFilename = "simple-merge.comet.yml",
        sourceDomainOrJobPathname = s"/sample/merge/simple-merge.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players-merge.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending

        val accepted: Array[Row] = sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/Players")
          .collect()

        // Input contains a row with an older timestamp
        // Without MergeOptions.timestamp set, the existing data should be overridden anyway

        val expected: Array[Row] =
          sparkSession.read
            .option("header", "false")
            .option("encoding", "UTF-8")
            .schema(playerSchema)
            .csv(getResPath("/expected/datasets/accepted/DOMAIN/Players-always-override.csv"))
            .collect()

        accepted should contain theSameElementsAs expected
      }
    }
    "Ingest updated schema with merge" should "produce merged results accepted" in {
      new SpecTrait(
        domainOrJobFilename = "simple-merge.comet.yml",
        sourceDomainOrJobPathname = s"/sample/merge/simple-merge.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
      }

      new SpecTrait(
        domainOrJobFilename = "merge-with-new-schema.comet.yml",
        sourceDomainOrJobPathname = s"/sample/merge/merge-with-new-schema.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/merge/Players-Entitled.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending

        val accepted: Array[Row] = sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/Players")
          .collect()

        // Input contains a row with an older timestamp
        // With MergeOptions.timestamp set, that row should be ignored (the rest should be updated)

        val expected: Array[Row] =
          sparkSession.read
            .option("encoding", "UTF-8")
            .schema(
              "`PK` STRING,`firstName` STRING,`lastName` STRING,`DOB` DATE,`title` STRING,`YEAR` INT,`MONTH` INT"
            )
            .csv(getResPath("/expected/datasets/accepted/DOMAIN/Players-merged-entitled.csv"))
            .collect()

        accepted should contain theSameElementsAs expected
      }
    }

    "A postsql query" should "update the resulting schema" in {
      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = s"/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/employee.csv"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
        val acceptedDf: DataFrame = sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/employee")
        acceptedDf.schema.fields.length shouldBe 1
        acceptedDf.schema.fields.map(_.name).count("name".equals) shouldBe 1

      }
    }
    "Ingest Dream Contact CSV" should "produce file in accepted" in {
      new SpecTrait(
        domainOrJobFilename = "dream.comet.yml",
        sourceDomainOrJobPathname = s"/sample/dream/dream.comet.yml",
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

        // If we run this test alone, we do not have rejected, else we have rejected but not accepted ...
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

        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/dream/client.json"))
            // Timezone Problem
            .drop("customer_creation_date")
            .withColumn("truncated_zip_code", substring(col("zip_code"), 0, 3))
            .withColumn("source_file_name", lit("OneClient_Contact_20190101_090800_008.psv"))

        acceptedDf.except(expectedAccepted).count() shouldBe 0
      }

    }

    "Ingest Dream Segment CSV" should "produce file in accepted" in {

      new SpecTrait(
        domainOrJobFilename = "dream.comet.yml",
        sourceDomainOrJobPathname = "/sample/dream/dream.comet.yml",
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
        domainOrJobFilename = "locations.comet.yml",
        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.comet.yml",
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
    "Ingest Flat Locations JSON" should "produce file in accepted" in {

      new SpecTrait(
        domainOrJobFilename = "locations.comet.yml",
        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.comet.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        cleanMetadata
        cleanDatasets

        loadPending

        // Accepted should have the same data as input
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/$datasetDomainName/flat_locations/$getTodayPartitionPath"
          )

        val expectedAccepted =
          sparkSession.read
            .json(
              getResPath("/expected/datasets/accepted/locations/locations.json")
            )
            .withColumn("name_upper_case", upper(col("name")))
            .withColumn("source_file_name", lit("locations.json"))

        acceptedDf.show(false)
        expectedAccepted.show(false)
        acceptedDf
          .select(col("id"))
          .except(expectedAccepted.select(col("id")))
          .count() shouldBe 0

      }

    }
    "Ingest Locations XML" should "produce file in accepted" in {

      new SpecTrait(
        domainOrJobFilename = "locations.comet.yml",
        sourceDomainOrJobPathname = s"/sample/xml/locations.comet.yml",
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

        import sparkSession.implicits._
        val (seconds, millis) =
          acceptedDf
            .select($"seconds", $"millis")
            .filter($"name" like "Paris")
            .as[(String, String)]
            .collect()
            .head

        // We just check against the year since the test may be executed in a different time zone :)
        seconds.substring(0, 4) shouldBe "2021"
        millis.substring(0, 4) shouldBe "1970"
      }
    }
    "Load Business with Transform Tag" should "load an AutoDesc" in {
      new SpecTrait(
        domainOrJobFilename = "locations.comet.yml",
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations.comet.yml",
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
        job.success.value.name shouldBe "business" // Job renamed to filename and error is logged
      }
    }

    // TODO TOFIX
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
        domainOrJobFilename = "locations.comet.yml",
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations.comet.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        cleanDatasets

        val schemaHandler = new SchemaHandler(storageHandler)

        val schema: Option[Schema] = schemaHandler.domains
          .find(_.name == "locations")
          .flatMap(_.tables.find(_.name == "locations"))
        val expected: String =
          """
            |{
            |  "index_patterns": ["locations.locations", "locations.locations-*"],
            |  "settings": {
            |    "number_of_shards": "1",
            |    "number_of_replicas": "0"
            |  },
            |  "mappings": {
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
            |  }
            |}
        """.stripMargin.trim
        val mapping =
          schema.map(_.esMapping(None, "locations", schemaHandler)).map(_.trim).getOrElse("")
        logger.info(mapping)
        mapping.replaceAll("\\s", "") shouldBe expected.replaceAll("\\s", "")
      }

    }
    "JSON Schema" should "produce valid template" in {
      new SpecTrait(
        domainOrJobFilename = "locations.comet.yml",
        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.comet.yml",
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
        format = Some(ai.starlake.schema.model.Format.POSITION),
        encoding = Some("ISO-8859-1"),
        withHeader = Some(false),
        sink = Some(BigQuerySink(timestamp = Some("_PARTITIONTIME"))),
        write = Some(WriteMode.OVERWRITE)
      )
    }
    "Exporting domain as Dot" should "create a valid dot file" in {
      new SpecTrait(
        domainOrJobFilename = "dream.comet.yml",
        sourceDomainOrJobPathname = s"/sample/dream/dream.comet.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        cleanMetadata
        cleanDatasets
        val schemaHandler = new SchemaHandler(settings.storageHandler)

        new Yml2GraphViz(schemaHandler).run(Array("--all", "false"))

        val tempFile = File.newTemporaryFile().pathAsString
        new Yml2GraphViz(schemaHandler).run(
          Array("--all", "true", "--output", tempFile)
        )
        val fileContent = readFileContent(tempFile)
        val expectedFileContent = loadTextFile("/expected/dot/output.dot")
        fileContent shouldBe expectedFileContent

        val result = schemaHandler.domains.head.asDot(false)
        result.trim shouldBe """
                               |
                               |dream_segment [label=<
                               |<table border="0" cellborder="1" cellspacing="0">
                               |<tr><td port="0" bgcolor="darkgreen"><B><FONT color="white"> segment </FONT></B></td></tr>
                               |<tr><td port="dream_id"><B> dreamkey:long </B></td></tr>
                               |</table>>];
                               |
                               |
                               |
                               |dream_client [label=<
                               |<table border="0" cellborder="1" cellspacing="0">
                               |<tr><td port="0" bgcolor="darkgreen"><B><FONT color="white"> client </FONT></B></td></tr>
                               |<tr><td port="dream_id"><I> dream_id:long </I></td></tr>
                               |</table>>];
                               |
                               |dream_client:dream_id -> dream_segment:0
                               |
                               |""".stripMargin.trim
      }
    }

    "Ingest Dream Contact CSV with ignore" should "produce file in accepted" in {
      new SpecTrait(
        domainOrJobFilename = "dreamignore.comet.yml",
        sourceDomainOrJobPathname = s"/sample/dream/dreamignore.comet.yml",
        datasetDomainName = "dreamignore",
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

        // If we run this test alone, we do not have rejected, else we have rejected but not accepted ...
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

        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/dream/clientignore.json"))
            // Timezone Problem
            .drop("customer_creation_date")
            .withColumn("truncated_zip_code", substring(col("zip_code"), 0, 3))
            .withColumn("source_file_name", lit("OneClient_Contact_20190101_090800_008.psv"))

        acceptedDf.columns.length shouldBe expectedAccepted.columns.length
        acceptedDf.except(expectedAccepted).count() shouldBe 0
      }

    }
    "Schema with external refs" should "produce import external refs into domain" in {
      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = s"/sample/schema-refs/WITH_REF.comet.yml",
        datasetDomainName = "WITH_REF",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        cleanDatasets

        withSettings.deliverTestFile(
          "/sample/schema-refs/_players.comet.yml",
          new Path(domainMetadataRootPath, "_players.comet.yml")
        )

        withSettings.deliverTestFile(
          "/sample/schema-refs/_users.comet.yml",
          new Path(domainMetadataRootPath, "_users.comet.yml")
        )
        val schemaHandler = new SchemaHandler(settings.storageHandler)
        schemaHandler
          .getDomain("WITH_REF")
          .map(_.tables.map(_.name))
          .get should contain theSameElementsAs List(
          "User",
          "Players",
          "employee"
        )
      }
    }

  }
}
