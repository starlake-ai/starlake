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

import java.nio.charset.Charset

import ai.starlake.TestHelper

import scala.io.Codec

class PositionIngestionJobSpec extends TestHelper {
  "Ingest Position File" should "should be ingested from pending to accepted, and archived" in {
    import org.slf4j.impl.StaticLoggerBinder
    val binder = StaticLoggerBinder.getSingleton
    logger.debug(binder.getLoggerFactory.toString)
    logger.debug(binder.getLoggerFactoryClassStr)

    new WithSettings {
      new SpecTrait(
        domainOrJobFilename = "position.comet.yml",
        sourceDomainOrJobPathname = "/sample/position/position.comet.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/XPOSTBL"
      ) {
        cleanMetadata
        cleanDatasets

        logger.info(settings.comet.datasets)
        loadPending

        // Check archive

        readFileContent(
          cometDatasetsPath + s"/archive/${datasetDomainName}/XPOSTBL"
        ) shouldBe loadTextFile(
          "/sample/position/XPOSTBL"
        )

        // Accepted should have the same data as input
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/${datasetDomainName}/account/${getTodayPartitionPath}"
          )
        printDF(acceptedDf, "acceptedDf")
        acceptedDf.count() shouldBe
        sparkSession.read
          .text(getClass.getResource(s"/sample/${datasetDomainName}/XPOSTBL").toURI.getPath)
          .count()
        acceptedDf.schema.fields.map(_.name).contains("calculatedCode") shouldBe true
        acceptedDf.schema.fields.map(_.name).contains("fileName") shouldBe true
      }

    }
  }
  "Ingestion of empty Position file" should "run without errors" in {
    new WithSettings {
      new SpecTrait(
        domainOrJobFilename = "position.comet.yml",
        sourceDomainOrJobPathname = "/sample/position/position.comet.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/empty_position"
      ) {
        cleanMetadata
        cleanDatasets
        logger.info(settings.comet.datasets)
        loadPending shouldBe true
      }
    }
  }
  "Ingest Position File" should "use encoding when loading files" in {
    new WithSettings {
      new SpecTrait(
        domainOrJobFilename = "positionWithEncoding.comet.yml",
        sourceDomainOrJobPathname = "/sample/positionWithEncoding/positionWithEncoding.comet.yml",
        datasetDomainName = "positionWithEncoding",
        sourceDatasetPathName = "/sample/positionWithEncoding/data-iso88591.dat"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending(new Codec(Charset forName "ISO-8859-1"))
        // Accepted should contain data formatted correctly
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/${datasetDomainName}/DATA"
          )
        acceptedDf.filter(acceptedDf("someData").contains("spécifié")).count() shouldBe 1
      }
    }
  }
  "Ingest Position Regex File with ignore string" should "ignore first line" in {
    new WithSettings {
      new SpecTrait(
        domainOrJobFilename = "positionWithIgnore.comet.yml",
        sourceDomainOrJobPathname = "/sample/positionWithIgnore/positionWithIgnore.comet.yml",
        datasetDomainName = "positionWithIgnore",
        sourceDatasetPathName = "/sample/positionWithIgnore/dataregex-ignore.dat"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
        // Accepted should contain data formatted correctly
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/${datasetDomainName}/DATAREGEX"
          )
        acceptedDf.count() shouldBe 1
      }
    }
  }

  "Ingest Position UDF File with ignore string" should "ignore first line" in {
    new WithSettings {
      new SpecTrait(
        domainOrJobFilename = "positionWithIgnore.comet.yml",
        sourceDomainOrJobPathname = "/sample/positionWithIgnore/positionWithIgnore.comet.yml",
        datasetDomainName = "positionWithIgnore",
        sourceDatasetPathName = "/sample/positionWithIgnore/dataudf-ignore.dat"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
        // Accepted should contain data formatted correctly
        val acceptedDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/${datasetDomainName}/DATAUDF"
          )
        acceptedDf.count() shouldBe 1
      }
    }
  }

}
