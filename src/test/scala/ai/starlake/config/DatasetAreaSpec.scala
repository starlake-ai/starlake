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

package ai.starlake.config

import ai.starlake.TestHelper
import ai.starlake.job.bootstrap.Bootstrap
import better.files.File

class DatasetAreaSpec extends TestHelper {

  new WithSettings() {
    if (false) {
      "bootstrap" should "generate data correctly" in {
        Bootstrap.bootstrap(None)
        assertCommonStructure(settings)
        assertFoldersExist(
          settings,
          List("out", "diagrams/load", "diagrams/acl", "diagrams/transform", "metadata", "incoming")
        )
      }

      "bootstrap quickstart" should "generate data correctly" in {
        Bootstrap.bootstrap(Some("quickstart"))
        assertCommonStructure(settings)
        assertExistence(
          settings,
          Nil,
          List(
            "metadata/transform/kpi/_config.sl.yml",
            "metadata/transform/kpi/customers_kpi.sql",
            "metadata/types/default.sl.yml",
            "metadata/application.sl.yml",
            "metadata/env.sl.yml",
            "incoming/customers-2018-01-01.psv"
          )
        )
        assertNoFilesInFolder(
          settings,
          List("out", "diagrams/load", "diagrams/acl", "diagrams/transform")
        )
      }

      "bootstrap userguide" should "generate data correctly" in {
        Bootstrap.bootstrap(Some("userguide"))
        assertCommonStructure(settings)
        assertExistence(
          settings,
          Nil,
          List(
            "metadata/load/hr/_config.sl.yml",
            "metadata/load/hr/locations.sl.yml",
            "metadata/load/hr/sellers.sl.yml",
            "metadata/load/sales/_config.sl.yml",
            "metadata/load/sales/customers.sl.yml",
            "metadata/load/sales/orders.sl.yml",
            "metadata/transform/sales_kpi/_config.sl.yml",
            "metadata/transform/sales_kpi/byseller_kpi.sql",
            "metadata/types/default.sl.yml",
            "metadata/types/types.sl.yml",
            "metadata/env.sl.yml",
            "metadata/env.BQ.sl.yml",
            "metadata/env.FS.sl.yml",
            "metadata/application.sl.yml",
            "incoming/locations-2018-01-01.json",
            "incoming/sellers-2018-01-01.json",
            "incoming/customers-2018-01-01.psv",
            "incoming/orders-2018-01-01.csv"
          )
        )
        assertNoFilesInFolder(
          settings,
          List("out", "diagrams/load", "diagrams/acl", "diagrams/transform")
        )
      }
    }
  }

  private def assertCommonStructure(settings: Settings): Unit = {
    assertExistence(
      settings,
      List("out", "diagrams/load", "diagrams/acl", "diagrams/transform", "metadata", "incoming"),
      List(".vscode/extensions.json")
    )
  }
  private def assertExistence(
    settings: Settings,
    expectedFolders: List[String],
    expectedFiles: List[String]
  ): Unit = {
    val rootFolder = File(settings.appConfig.metadata).parent
    val checkFileStatus = (l: List[String]) =>
      l.map(rootFolder / _).map(f => f -> FileStatus(f.exists, f.isDirectory))
    all(checkFileStatus(expectedFiles)) should have(
      '_2(FileStatus(exist = true, isDirectory = false))
    )
    all(checkFileStatus(expectedFolders)) should have(
      '_2(FileStatus(exist = true, isDirectory = true))
    )
  }

  private def assertNoFilesInFolder(
    settings: Settings,
    expectedEmptyFolders: List[String]
  ): Unit = {
    val rootFolder = File(settings.appConfig.metadata).parent
    all(
      expectedEmptyFolders.map(rootFolder / _).map { f =>
        println(f.list.toList)
        println(f.list.exists(_.isRegularFile))
        f -> !f.list.exists(_.isRegularFile)
      }
    ) should have(
      '_2(true)
    )
  }

  private def assertFoldersExist(
    settings: Settings,
    folders: List[String]
  ): Unit = {
    val rootFolder = File(settings.appConfig.metadata).parent
    all(folders.map(rootFolder / _).map { f => f -> f.exists }) should have('_2(true))
  }

  private case class FileStatus(exist: Boolean, isDirectory: Boolean)
}
