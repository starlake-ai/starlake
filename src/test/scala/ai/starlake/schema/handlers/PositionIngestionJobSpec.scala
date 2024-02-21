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
import better.files.File
import org.apache.spark.sql.catalyst.TableIdentifier

import java.nio.charset.Charset
import scala.io.Codec

class PositionIngestionJobSpec extends TestHelper {
  "Ingest Position File" should "should be ingested from pending to accepted, and archived" in {
    import org.slf4j.impl.StaticLoggerBinder
    val binder = StaticLoggerBinder.getSingleton
    logger.debug(binder.getLoggerFactory.toString)
    logger.debug(binder.getLoggerFactoryClassStr)

    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/position/position.sl.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/XPOSTBL"
      ) {
        sparkSession.sql("DROP DATABASE IF EXISTS position CASCADE")
        cleanMetadata
        cleanDatasets

        logger.info(settings.appConfig.datasets)
        loadPending
        val location = getTablePath("position", "account")
        println(starlakeDatasetsPath + s"/archive/${datasetDomainName}/XPOSTBL")

        // Check archive
        readFileContent(
          starlakeDatasetsPath + s"/archive/${datasetDomainName}/XPOSTBL"
        ) shouldBe loadTextFile(
          "/sample/position/XPOSTBL"
        )

        // Accepted should have the same data as input
        println(s"$location/$getTodayPartitionPath")
        val acceptedDf = sparkSession.read
          .format(settings.appConfig.defaultWriteFormat)
          .load(
            s"$location/$getTodayPartitionPath"
          )
        printDF(acceptedDf, "acceptedDf")
        acceptedDf.count() shouldBe
        sparkSession.read
          .text(File(getClass.getResource(s"/sample/${datasetDomainName}/XPOSTBL")).pathAsString)
          .count()
        acceptedDf.schema.fields.map(_.name).contains("calculatedCode") shouldBe true
        acceptedDf.schema.fields.map(_.name).contains("fileName") shouldBe true
      }

    }
  }
  "Ingestion of empty Position file" should "run without errors" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/position/position.sl.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/empty_position"
      ) {
        cleanMetadata
        cleanDatasets
        logger.info(settings.appConfig.datasets)
        loadPending.isSuccess shouldBe true
      }
    }
  }
  "Ingest Position File" should "use encoding when loading files" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionWithEncoding/positionWithEncoding.sl.yml",
        datasetDomainName = "positionWithEncoding",
        sourceDatasetPathName = "/sample/positionWithEncoding/data-iso88591.dat"
      ) {
        sparkSessionReset(settings)
        cleanMetadata
        cleanDatasets
        loadPending(new Codec(Charset forName "ISO-8859-1"))

        val tblMetadata = sparkSession.sessionState.catalog.getTableMetadata(
          new TableIdentifier("DATA", Some("positionWithEncoding"))
        )
        val location = tblMetadata.location.getPath

        // Accepted should contain data formatted correctly
        val acceptedDf = sparkSession.read
          .format(settings.appConfig.defaultWriteFormat)
          .load(
            location
          )
        acceptedDf.filter(acceptedDf("someData").contains("spécifié")).count() shouldBe 1
      }
    }
  }
  "Ingest Position Regex File with ignore string" should "ignore first line" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionWithIgnore/positionWithIgnore.sl.yml",
        datasetDomainName = "positionWithIgnore",
        sourceDatasetPathName = "/sample/positionWithIgnore/dataregex-ignore.dat"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
        // Accepted should contain data formatted correctly
        val tblMetadata = sparkSession.sessionState.catalog.getTableMetadata(
          new TableIdentifier("DATAREGEX", Some(datasetDomainName))
        )
        val location = tblMetadata.location.getPath

        val acceptedDf =
          sparkSession.read
            .format(settings.appConfig.defaultWriteFormat)
            .load(location)
        acceptedDf.count() shouldBe 1
      }
    }
  }

  "Ingest Position UDF File with ignore string" should "ignore first line" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionWithIgnore/positionWithIgnore.sl.yml",
        datasetDomainName = "positionWithIgnore",
        sourceDatasetPathName = "/sample/positionWithIgnore/dataudf-ignore.dat"
      ) {
        sparkSession.sql("DROP DATABASE IF EXISTS positionWithIgnore CASCADE")
        cleanMetadata
        cleanDatasets
        loadPending
        // Accepted should contain data formatted correctly
        val acceptedDf = sparkSession.table(s"${datasetDomainName}.DATAUDF")
        acceptedDf.count() shouldBe 1
      }
    }
  }

}
