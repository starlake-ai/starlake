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
import ai.starlake.extract.JdbcDbUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata => _, _}

class Schema3HandlerSpec extends TestHelper {

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

  "Ingest Dream Contact CSV with ignore" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dream/dreamignore.sl.yml",
        datasetDomainName = "dreamignore",
        sourceDatasetPathName = "/sample/dream/OneClient_Contact_20190101_090800_008.psv"
      ) {

        cleanMetadata
        TestHelper.closeSession()
        cleanDatasets
        deliverSourceDomain()
        List("/sample/dream/client.sl.yml", "/sample/dream/segment.sl.yml").foreach(
          deliverSourceTable
        )
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
