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
import ai.starlake.job.ingest.LoadConfig
import better.files.File
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata => _, _}

import scala.reflect.io.Directory

class SchemaInfo1HandlerSpec extends TestHelper {

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
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        println(s"test root is $starlakeTestRoot")
        File(starlakeTestRoot).delete()
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
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
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
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
        sourceDomainOrJobPathname = "/sample/xml/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/xml/locations.xml"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable("/sample/xml/locations.sl.yml")
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
        sourceDomainOrJobPathname = "/sample/xsd/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/xsd/locations.xml"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable("/sample/xsd/locations.sl.yml")

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
        sourceDomainOrJobPathname = "/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/User.sl.yml",
          "/sample/Players.sl.yml",
          "/sample/employee.sl.yml",
          "/sample/complexUser.sl.yml"
        ).foreach(deliverSourceTable)
        getDomain("DOMAIN").foreach { domain =>
          val result = domain.tables.map { table =>
            table.finalName -> table.containsArrayOfRecords()
          }
          val expected =
            List(("User", false), ("Players", false), ("employee", false), ("complexUser", true))
          result should contain theSameElementsAs expected
        }

        private val validator = loadWorkflow("DOMAIN", "/sample/Players.csv")
        validator.load(
          LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
        )

        deleteSourceDomains()
        deliverSourceDomain("DOMAIN", "/sample/merge/merge-with-timestamp.sl.yml")
        deliverSourceTable(
          "DOMAIN",
          "/sample/merge/PlayersTimestamp.sl.yml",
          Some("Players.sl.yml")
        )
        private val validator2 = loadWorkflow("DOMAIN", "/sample/Players-merge.csv")
        validator2.load(
          LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
        )

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
        deliverSourceTable("DOMAIN", "/sample/merge/PlayersSimple.sl.yml", Some("Players.sl.yml"))

        private val validator3 = loadWorkflow("DOMAIN", "/sample/Players-merge.csv")
        validator3.load(
          LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
        )

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
        sourceDomainOrJobPathname = "/sample/merge/simple-merge.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable("DOMAIN", "/sample/merge/PlayersSimple.sl.yml", Some("Players.sl.yml"))
        loadPending
        cleanMetadata

        deliverSourceDomain("DOMAIN", "/sample/merge/merge-with-new-schema.sl.yml")
        deliverSourceTable("DOMAIN", "/sample/merge/Players.sl.yml")
        private val validator = loadWorkflow("DOMAIN", "/sample/merge/Players-Entitled.csv")
        validator.load(
          LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
        )

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

}
