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
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.{IngestConfig, LoadConfig}
import ai.starlake.schema.generator.{AclDependencies, TableDependencies}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter.RichFormatter
import better.files.File
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata => _, _}
import org.apache.spark.sql.{DataFrame, Row}

import java.net.URL
import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

class SchemaHandlerSpec extends TestHelper {

  override def afterAll(): Unit = {
    super.afterAll()
    // We need to start it manually because we need to access the HTTP mapped port
    // in the configuration below before any test get executed.
    esContainer.stop()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestHelper.closeSession()
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

  lazy val esConfiguration: Config = {
    val port = esContainer.httpHostAddress.substring(
      esContainer.httpHostAddress.lastIndexOf(':') + 1
    )
    println(s"--------------------Elasticsearch port: $port-------------------")
    ConfigFactory
      .parseString(s"""
           |connectionRef = "elasticsearch"
           |audit.sink.connectionRef = "spark"
           |connections.elasticsearch {
           |  type = "elasticsearch"
           |  sparkFormat = "elasticsearch"
           |  mode = "Append"
           |  options = {
           |    "es.nodes.wan.only": "true"
           |    "es.nodes": "localhost"
           |    "es.port": $port,

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
  }

  "Ingest Flat Locations JSON" should "produce file in accepted" in {
    new WithSettings() {
      // clean datasets folder
      new Directory(new java.io.File(starlakeDatasetsPath)).deleteRecursively()

      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        println(s"test root is $starlakeTestRoot")
        File(starlakeTestRoot).delete()
        sparkSessionReset(settings)
        cleanMetadata
        deliverSourceDomain()
        loadPending

        // Accepted should have the same data as input
        val acceptedDf = sparkSession
          .sql(s"select * from $datasetDomainName.flat_locations where $getTodayCondition")
          .drop("year", "month", "day")

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
        sparkSession.sql("DROP TABLE IF EXISTS locations.flat_locations").show()
      }
    }
  }
  "Ingest Locations JSON" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/${settings.appConfig.area.archive}/$datasetDomainName/locations.json"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Accepted should have the same data as input
        val acceptedDf =
          sparkSession
            .sql(s"select * from $datasetDomainName.locations where $getTodayCondition")
            .drop("year", "month", "day")

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
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }

  "Ingest Locations XML" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/xml/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/xml/locations.xml"
      ) {
        cleanMetadata
        deliverSourceDomain()

        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/${settings.appConfig.area.archive}/$datasetDomainName/locations.xml"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Accepted should have the same data as input
        val acceptedDf = sparkSession
          .sql(s"select * from $datasetDomainName.locations")
          .drop("year", "month", "day")

        val session = sparkSession

        import session.implicits._

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
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }
  "Ingest Locations XML with XSD" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/xsd/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/xsd/locations.xml"
      ) {
        cleanMetadata
        deliverSourceDomain()

        withSettings.deliverTestFile(
          "/sample/xsd/locations.xsd",
          new Path(DatasetArea.metadata, "sample/xsd/locations.xsd")
        )

        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/${settings.appConfig.area.archive}/$datasetDomainName/locations.xml"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        val acceptedDf =
          sparkSession.sql(s"select * from $datasetDomainName.locations where $getTodayCondition")

        val session = sparkSession

        import session.implicits._

        val (seconds, millis) =
          acceptedDf
            .select($"seconds", $"millis")
            .filter($"name" like "Paris")
            .as[(String, String)]
            .collect()
            .head

        // We just check against the year since the test may be executed in a different time zone :)
        seconds.substring(0, 4) shouldBe "1631"
        millis.substring(0, 4) shouldBe "1631"
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }

  "Ingest schema with merge" should "succeed" in {
    new WithSettings(
      testConfiguration.withValue("grouped", ConfigValueFactory.fromAnyRef("false"))
    ) {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        getDomain("DOMAIN").foreach { domain =>
          val result = domain.tables.map { table =>
            table.finalName -> table.containsArrayOfRecords()
          }
          val expected =
            List(("User", false), ("Players", false), ("employee", false), ("complexUser", true))
          result should contain theSameElementsAs expected
        }

        private val validator = loadWorkflow("DOMAIN", "/sample/Players.csv")
        validator.load(LoadConfig(accessToken = None, test = false, files = None))

        deleteSourceDomains()
        deliverSourceDomain("DOMAIN", "/sample/merge/merge-with-timestamp.sl.yml")
        private val validator2 = loadWorkflow("DOMAIN", "/sample/Players-merge.csv")
        validator2.load(LoadConfig(accessToken = None, test = false, files = None))

        /*
        val accepted: Array[Row] = sparkSession.read
          .parquet(starlakeDatasetsPath + s"/accepted/$datasetDomainName/Players")
          .collect()
         */
        val accepted: Array[Row] = sparkSession
          .sql(s"select * from $datasetDomainName.Players")
          .collect()

        // Input contains a row with an older timestamp
        // With MergeOptions.timestamp set, that row should be ignored (the rest should be updated)

        val expected: Array[Row] =
          sparkSession.read
            .option("header", "false")
            .option("encoding", "UTF-8")
            .schema(playerSchema)
            .csv(
              getResPath("/expected/datasets/accepted/DOMAIN/Players-merged-with-timestamp.csv")
            )
            .collect()

        accepted should contain theSameElementsAs expected

        deleteSourceDomains()
        deliverSourceDomain("DOMAIN", "/sample/merge/simple-merge.sl.yml")

        private val validator3 = loadWorkflow("DOMAIN", "/sample/Players-merge.csv")
        validator3.load(LoadConfig(accessToken = None, test = false, files = None))

        /*        val accepted2: Array[Row] = sparkSession.read
          .parquet(starlakeDatasetsPath + s"/accepted/$datasetDomainName/Players")
          .collect()
         */
        val accepted2: Array[Row] =
          sparkSession.sql(s"select * from $datasetDomainName.Players").collect()
        // Input contains a row with an older timestamp
        // Without MergeOptions.timestamp set, the existing data should be overridden anyway

        val expected2: Array[Row] =
          sparkSession.read
            .option("header", "false")
            .option("encoding", "UTF-8")
            .schema(playerSchema)
            .csv(getResPath("/expected/datasets/accepted/DOMAIN/Players-always-override.csv"))
            .collect()

        accepted2 should contain theSameElementsAs expected2
        deleteSourceDomain("DOMAIN", "/sample/merge/simple-merge.sl.yml")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User").show()
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players").show()
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser").show()
      }
    }
  }

  "Ingest updated schema with merge" should "produce merged results accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/merge/simple-merge.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending
        cleanMetadata

        deliverSourceDomain("DOMAIN", "/sample/merge/merge-with-new-schema.sl.yml")
        private val validator = loadWorkflow("DOMAIN", "/sample/merge/Players-Entitled.csv")
        validator.load(LoadConfig(accessToken = None, test = false, files = None))

        val accepted: Array[Row] = sparkSession
          .sql(s"select PK, firstName, lastName, DOB, YEAR, MONTH from $datasetDomainName.Players")
          .collect()
        // Input contains a row with an older timestamp
        // With MergeOptions.timestamp set, that row should be ignored (the rest should be updated)

        accepted.foreach(println)
        val expected =
          sparkSession.read
            .option("encoding", "UTF-8")
            .schema(
              "`PK` STRING,`firstName` STRING,`lastName` STRING,`DOB` DATE,`title` STRING,`YEAR` INT,`MONTH` INT"
            )
            .csv(getResPath("/expected/datasets/accepted/DOMAIN/Players-merged-entitled.csv"))

        expected.createOrReplaceTempView("expected")
        val expectedFinalDf =
          sparkSession
            .sql("select PK, firstName, lastName, DOB, YEAR, MONTH from expected")
            .collect()
        expectedFinalDf.foreach(println)
        accepted should contain theSameElementsAs expectedFinalDf
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser")
      }
    }
  }

  "Ingesting data" should "adapt write based on file attributes" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/adaptiveWrite/simple-adaptive-write.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        cleanDatasets
        TestHelper.closeSession()
        deliverSourceDomain()
        loadPending

        // We are by  default in ingestion Time strategy
        // Since full is loaded first, it will be the base for the delta

        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-FULL.csv")
        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-DELTA.csv").load(
          LoadConfig(accessToken = None, test = false, files = None)
        )

        val acceptedFullDelta: Array[Row] = sparkSession
          .sql(
            s"select PK, firstname, lastName, DOB, YEAR, MONTH, title from $datasetDomainName.Players"
          )
          .collect()

        val expectedFullDelta: Array[Row] =
          sparkSession.read
            .option("encoding", "UTF-8")
            .schema(
              "`PK` STRING,`firstName` STRING,`lastName` STRING,`DOB` DATE,`YEAR` STRING,`MONTH` STRING,`title` STRING"
            )
            .csv(
              getResPath("/expected/datasets/accepted/DOMAIN/Players-adaptive-write-FULL-DELTA.csv")
            )
            .collect()

        acceptedFullDelta should contain theSameElementsAs expectedFullDelta
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser")
        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-DELTA.csv")
        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-FULL.csv").load(
          LoadConfig(accessToken = None, test = false, files = None)
        )

        val acceptedDeltaFull: Array[Row] = sparkSession
          .sql(
            s"select PK, firstname, lastName, DOB, YEAR, MONTH, title from $datasetDomainName.Players"
          )
          .collect()

        val expectedDeltaFull: Array[Row] =
          sparkSession.read
            .option("encoding", "UTF-8")
            .schema(
              "`PK` STRING,`firstName` STRING,`lastName` STRING,`DOB` DATE,`YEAR` STRING,`MONTH` STRING,`title` STRING"
            )
            .csv(
              getResPath("/expected/datasets/accepted/DOMAIN/Players-adaptive-write-DELTA-FULL.csv")
            )
            .collect()

        acceptedDeltaFull should contain theSameElementsAs expectedDeltaFull
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser")
      }
    }
  }
  /*
  new WithSettings(esConfiguration) {
    // TODO Helper (to delete)
    "Ingest CSV" should "produce file in accepted" in {
      // ES Load in standby
      pending
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/elasticsearch/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {

        cleanMetadata
        deliverSourceDomain()

        assert(loadPending.isSuccess)

        // Check Archived
        readFileContent(
          starlakeDatasetsPath + s"/archive/$datasetDomainName/SCHEMA-VALID.dsv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        val rejectedDf = sparkSession.sql("select * from audit.rejected")

        val expectedRejectedF =
          sparkSession.read
            .schema(prepareSchema(rejectedDf.schema))
            .json(getResPath("/expected/datasets/rejected/DOMAIN.json"))

        expectedRejectedF.except(rejectedDf).count() shouldBe 1

        // Accepted should have the same data as input
        val client = HttpClients.createDefault
        val acceptedUri = s"http://${esContainer.httpHostAddress}/domain.user/_search?pretty"
        val acceptedgetRequest = new HttpGet(acceptedUri)
        acceptedgetRequest.setHeader("Content-Type", "application/json")
        val acceptedResponse = client.execute(acceptedgetRequest)
        val responseString: String = EntityUtils.toString(acceptedResponse.getEntity, "UTF-8")
        assert(responseString.indexOf(""""age" : 121""") > 0)
        assert(responseString.indexOf(""""age" : 122""") > 0)
        println(responseString)
        val countUri = s"http://${esContainer.httpHostAddress}/domain.user/_count"
        val getRequest = new HttpGet(countUri)
        getRequest.setHeader("Content-Type", "application/json")
        val response = client.execute(getRequest)

        response.getStatusLine.getStatusCode should be <= 299
        response.getStatusLine.getStatusCode should be >= 200
        EntityUtils.toString(response.getEntity()) contains "\"count\":2"

      }
    }
//    "load to elasticsearch" should "work" in {
//      new SpecTrait(
//        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.sl.yml",
//        datasetDomainName = "locations",
//        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
//      ) {
//        sparkSession.sql("DROP DATABASE IF EXISTS locations CASCADE")
//        cleanMetadata
//        deliverSourceDomain()
//        // loadPending
//        val validator = loadWorkflow()
//        val result = validator.esLoad(
//          ESLoadConfig(
//            domain = "DOMAIN",
//            schema = "",
//            format = "json",
//            dataset = Some(
//              Left(new Path(starlakeDatasetsPath + s"/pending/$datasetDomainName/locations.json"))
//            ),
//            options = settings.appConfig.connectionOptions("elasticsearch")
//          )
//        )
//        result.isSuccess shouldBe true
//      }
//    }
  }
   */

  "Ingest empty file with DSV schema" should "be ok " in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/employee-empty.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending.isSuccess shouldBe true
      }
    }
  }

  "load File" should "work" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {
        val targetPath = DatasetArea.path(
          DatasetArea.stage("DOMAIN.sl.yml"),
          new Path("/sample/SCHEMA-VALID.dsv").getName
        )
        cleanMetadata
        deliverSourceDomain()
        load(
          IngestConfig("DOMAIN.sl.yml", "User", List(targetPath), accessToken = None)
        ).isSuccess shouldBe true
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser")
      }
    }
  }

  "A postsql query" should "update the resulting schema" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/employee.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending
        val acceptedDf: DataFrame =
          sparkSession.sql(s"select distinct(name) from $datasetDomainName.employee")
        acceptedDf.count() shouldBe 1
        acceptedDf.collect().head.toString() shouldBe "[John]"
      }
    }
  }
  "Ingest Dream Contact CSV" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Contact_20190101_090800_008.psv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/archive/$datasetDomainName/OneClient_Contact_20190101_090800_008.psv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // If we run this test alone, we do not have rejected, else we have rejected but not accepted ...
        Try {
          printDF(
            sparkSession.sql(s"select * from ${datasetDomainName}_rejected.client"),
            "dream/client"
          )
        }

        // Accepted should have the same data as input
        val acceptedDf = sparkSession
          .sql(s"select * from $datasetDomainName.client where $getTodayCondition")
          // Timezone Problem
          .drop("customer_creation_date")
          .drop("year", "month", "day")

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
  }

  "Ingest schema with partition" should "produce partitioned output in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        loadPending
        println(starlakeDatasetsPath)

        val acceptedDf = sparkSession
          .sql(s"select * from $datasetDomainName.Players")

        val exceptedDf = sparkSession.read
          .option("header", "false")
          .schema(playerSchema)
          .csv(getResPath("/sample/Players.csv"))

        acceptedDf.except(exceptedDf).count() shouldBe 0

      }
    }
  }

  "Ingest Dream Segment CSV" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        cleanMetadata
        deliverSourceDomain()

        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/archive/$datasetDomainName/OneClient_Segmentation_20190101_090800_008.psv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Accepted should have the same data as input
        val acceptedDf =
          sparkSession
            .sql(s"select * from $datasetDomainName.segment where $getTodayCondition")
            .drop("year", "month", "day")

        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/dream/segment.json"))

        acceptedDf.except(expectedAccepted).count() shouldBe 0
      }
    }
  }

  "Load Business with Transform Tag" should "load an AutoDesc" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {

        import org.scalatest.TryValues._

        cleanMetadata
        deliverSourceDomain()
        val schemaHandler = new SchemaHandler(storageHandler)
        val filename = "/sample/metadata/transform/business/business.sl.yml"
        val jobPath = new Path(getClass.getResource(filename).toURI)
        val job = schemaHandler.loadJobTasksFromFile(jobPath)
        // FIXME, check why this works on master
        job.success.value.name shouldBe "business2"
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }
  "Load Transform Job" should "not reject tasks without SQL (SQL my be in external file)" in {
    new WithSettings() {
      cleanMetadata
      sparkSession.sql("DROP TABLE IF EXISTS locations.locations")
      sparkSession.sql("DROP TABLE IF EXISTS locations.flat_locations")
      val schemaHandler = new SchemaHandler(storageHandler)
      val filename = "/sample/job-tasks-without-sql/nosql.sl.yml"
      val jobPath = new Path(getClass.getResource(filename).toURI)

      val job = schemaHandler.loadJobTasksFromFile(jobPath)
      job.isFailure shouldBe false
    }
  }
  "Load Transform Job with taskrefs" should "succeed" in {
    new WithSettings() {
      cleanMetadata
      val schemaHandler = new SchemaHandler(storageHandler)
      val filename = "/sample/job-with-taskrefs/_config.sl.yml"
      val jobPath = new Path(getClass.getResource(filename).toURI)

      val job = schemaHandler.loadJobTasksFromFile(jobPath)
      job match {
        case Success(job) =>
          val tasks = job.tasks
          tasks.length shouldBe 3
          tasks.map(_.name) should contain theSameElementsAs (List(
            "dream2.client2", // tasks are handled before task refs
            "myjob.task1",
            "myjob.task2"
          ))
        case Failure(e) =>
          throw e
      }
    }
  }

  "Extract Var from Job File" should "find all vars" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {

        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
        cleanMetadata
        deliverSourceDomain()
        val schemaHandler = new SchemaHandler(storageHandler)
        val filename = "/sample/metadata/transform/business_with_vars/business_with_vars.sl.yml"
        val jobPath = new Path(getClass.getResource(filename).toURI)
        val content = storageHandler.read(jobPath)
        val vars = content.extractVars()
        vars should contain theSameElementsAs (Set("DOMAIN", "SCHEMA", "Y", "M"))
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }
  "Load Business with jinja" should "should not run jinja parser" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {

        import org.scalatest.TryValues._

        cleanMetadata
        deliverSourceDomain()
        val schemaHandler = new SchemaHandler(storageHandler)
        val filename = "/sample/metadata/transform/my-jinja-job/my-jinja-job.sl.yml"
        val jobPath = new Path(getClass.getResource(filename).toURI)
        val job = schemaHandler.loadJobTasksFromFile(jobPath)

        job.success.value.tasks.head.sql.get.trim shouldBe
        """{% set myList = ["col1,", "col2"] %}
              |select
              |{%- for x in myList %}
              |{{x}}
              |{%- endfor %}
              |from dream_working.client""".stripMargin // Job renamed to filename and error is logged
      }
    }
  }
  // TODO TOFIX
  //  "Load Business Definition" should "produce business dataset" in {
  //    val sh = new HdfsStorageHandler
  //    val jobsPath = new Path(DatasetArea.jobs, "sample/metadata/business/business.sl.yml")
  //    sh.write(loadFile("/sample/metadata/business/business.sl.yml"), jobsPath)
  //    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
  //    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
  //    validator.autoJob("business1")
  //  }

  "Writing types" should "work" in {
    new WithSettings() {

      val typesPath = new Path(DatasetArea.types, "types.sl.yml")

      deliverTestFile("/sample/types.sl.yml", typesPath)

      readFileContent(typesPath) shouldBe loadTextFile("/sample/types.sl.yml")
    }
  }
  "Mapping Schema" should "produce valid template" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        val schemaHandler = new SchemaHandler(storageHandler)

        val schema: Option[Schema] = schemaHandler
          .domains()
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
              |},
              |"year":{
              |  "type":"long"
              |},
              |"month":{
              | "type":"long"
              |},
              |"day": {
              |"type":"long"
              |}
              |}
              |  }
              |}
        """.stripMargin.trim
        val mapping =
          schema.map(_.esMapping(None, "locations", schemaHandler)).map(_.trim).getOrElse("")
        logger.info(mapping)
        mapping.replaceAll("\\s", "") shouldBe expected.replaceAll("\\s", "")
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }

  "JSON Schema" should "produce valid template" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()

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
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()

      }
      // TODO: we aren't actually testing anything here are we?
    }
  }

  "Custom mapping in Metadata" should "be read as a map" in {
    new WithSettings() {
      val sch = new SchemaHandler(storageHandler)
      val content =
        """withHeader: false
            |encoding: ISO-8859-1
            |format: POSITION
            |sink:
            |  partition: ["_PARTITIONTIME"]
            |writeStrategy:
            |  type: OVERWRITE
            |""".stripMargin
      val metadata = sch.mapper.readValue(content, classOf[Metadata])

      metadata shouldBe Metadata(
        format = Some(ai.starlake.schema.model.Format.POSITION),
        encoding = Some("ISO-8859-1"),
        withHeader = Some(false),
        sink = Some(BigQuerySink(partition = Some(List("_PARTITIONTIME"))).toAllSinks()),
        writeStrategy = Some(WriteStrategy(Some(WriteStrategyType.OVERWRITE)))
      )
    }
  }
  "Exporting domain as Dot" should "create a valid dot file" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        File(starlakeMetadataPath + "/load").delete(swallowIOExceptions = true)
        cleanMetadata
        deliverSourceDomain()
        val schemaHandler = new SchemaHandler(settings.storageHandler())

        val tempFile = File.newTemporaryFile().pathAsString
        new TableDependencies(schemaHandler).run(
          Array("--all", "--output", tempFile)
        )
        val fileContent = readFileContent(tempFile)
        val expectedFileContent = loadTextFile("/expected/dot/output.dot")
        fileContent.trim should equal(expectedFileContent.trim)
        val domains = schemaHandler.domains()
        val result = domains.head.asDot(false, Set("dream.segment", "dream.client"))
        println(result)
        result.trim shouldBe
        """
              |dream_segment [label=<
              |<table border="0" cellborder="1" cellspacing="0">
              |<tr>
              |<td port="0" bgcolor="#008B00"><B><FONT color="white"> segment </FONT></B></td>
              |</tr>
              |<tr><td port="dreamkey"><B> dreamkey:long </B></td></tr>
              |</table>>];
              |
              |
              |
              |dream_client [label=<
              |<table border="0" cellborder="1" cellspacing="0">
              |<tr>
              |<td port="0" bgcolor="#008B00"><B><FONT color="white"> client </FONT></B></td>
              |</tr>
              |<tr><td port="dream_id"><I> dream_id:long </I></td></tr>
              |</table>>];
              |
              |dream_client:dream_id -> dream_segment:0
              |
              |""".stripMargin.trim
      }
    }
  }
  "Exporting domain as ACL Dot" should "create a valid ACL dot file" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        File(starlakeMetadataPath + "/load").delete(swallowIOExceptions = true)
        cleanMetadata
        deliverSourceDomain()
        val schemaHandler = new SchemaHandler(settings.storageHandler())

        new AclDependencies(schemaHandler).run(Array("--all"))

        val tempFile = File.newTemporaryFile().pathAsString

        new AclDependencies(schemaHandler).run(
          Array("--all", "--output", tempFile)
        )

        val fileContent = readFileContent(tempFile)
        val expectedFileContent = loadTextFile("/expected/dot/acl-output.dot")
        fileContent.trim shouldBe expectedFileContent.trim
        sparkSession.sql("DROP TABLE IF EXISTS dream.segment").show()
      }
    }
  }

  "Ingest Dream Contact CSV with ignore" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/dream/dreamignore.sl.yml",
        datasetDomainName = "dreamignore",
        sourceDatasetPathName = "/sample/dream/OneClient_Contact_20190101_090800_008.psv"
      ) {

        cleanMetadata
        TestHelper.closeSession()
        cleanDatasets
        deliverSourceDomain()
        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/archive/$datasetDomainName/OneClient_Contact_20190101_090800_008.psv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        val auditConnectionOptions = settings.appConfig.audit.sink.getSink().getConnection().options
        JdbcDbUtils.withJDBCConnection(auditConnectionOptions) { conn =>
          // drop table using jdbc statement connection conn in the lines below
          val rs = conn
            .createStatement()
            .executeQuery(
              s"select count(*) from audit.rejected where domain = '$datasetDomainName' and schema = 'client'"
            )
          rs.next()
          rs.getInt(1) shouldBe 0
        }

        val acceptedDf =
          sparkSession
            .sql(s"select * from $datasetDomainName.client where $getTodayCondition")
            .drop("year", "month", "day")
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
        sparkSession.sql("DROP TABLE IF EXISTS dream.client").show()
      }
    }
  }
}
