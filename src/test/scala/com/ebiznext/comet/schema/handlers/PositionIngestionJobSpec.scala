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

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.{TestHelper, TypeToImport}

class PositionIngestionJobSpec extends TestHelper {
  "Ingest Position File" should "should be ingested from pending to accepted, and archived" in {
    import org.slf4j.impl.StaticLoggerBinder
    val binder = StaticLoggerBinder.getSingleton
    System.out.println(binder.getLoggerFactory)
    System.out.println(binder.getLoggerFactoryClassStr)

    new SpecTrait {
      cleanMetadata
      cleanDatasets
      override val domainFilename: String = "position.yml"
      override val sourceDomainPathname: String = "/sample/position/position.yml"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "default.yml",
          "/sample/default.yml"
        ),
        TypeToImport(
          "types.yml",
          "/sample/position/types.yml"
        )
      )
      override val datasetDomainName: String = "position"
      override val sourceDatasetPathName: String = "/sample/position/XPOSTBL"
      println(Settings.comet.datasets)
      loadPending

      // Check archive

      readFileContent(cometDatasetsPath + s"/archive/${datasetDomainName}/XPOSTBL") shouldBe loadFile(
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
    }

  }
  "Ingest Position File" should "should be ingested from pending to accepted, and archived" in {
    import org.slf4j.impl.StaticLoggerBinder
    val binder = StaticLoggerBinder.getSingleton
    System.out.println(binder.getLoggerFactory)
    System.out.println(binder.getLoggerFactoryClassStr)

    new SpecTrait {
      cleanMetadata
      cleanDatasets
      override val domainFilename: String = "position.yml"
      override val sourceDomainPathname: String = "/sample/position/position.yml"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "default.yml",
          "/sample/default.yml"
        ),
        TypeToImport(
          "types.yml",
          "/sample/position/types.yml"
        )
      )
      override val datasetDomainName: String = "position"
      override val sourceDatasetPathName: String = "/sample/position/XPOSTBL"
      println(Settings.comet.datasets)
      loadPending

      // Check archive

      readFileContent(cometDatasetsPath + s"/archive/${datasetDomainName}/XPOSTBL") shouldBe loadFile(
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
    }

  }
}
