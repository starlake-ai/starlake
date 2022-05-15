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

import ai.starlake.{JdbcChecks, TestHelper}

class JsonMultilineIngestionJobSpec extends TestHelper with JdbcChecks {

  "Ingest Complex Multiline JSON " should "should be ingested from pending to accepted, and archived " in {
    new WithSettings {

      new SpecTrait(
        domainOrJobFilename = "json-multiline.comet.yml",
        sourceDomainOrJobPathname = "/sample/jsonmultiline/json-multiline.comet.yml",
        datasetDomainName = "jsonmultiline",
        sourceDatasetPathName = "/sample/jsonmultiline/complex-multiline.json"
      ) {

        cleanMetadata
        cleanDatasets

        loadPending

        val schemaHandler = new SchemaHandler(settings.storageHandler)
        val schema = schemaHandler.getSchema("jsonmultiline", "sample_json").get
        val sparkSchema = schema.sparkSchemaWithoutScriptedFields(schemaHandler)

        // Accepted should have the same data as input
        val resultDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/${datasetDomainName}/sample_json/${getTodayPartitionPath}"
          )

        logger.info(resultDf.showString(truncate = 0))
        import sparkSession.implicits._
        resultDf
          .select($"email")
          .map(_.getString(0))
          .collect() should contain theSameElementsAs List(
          "comet-test@dummy.com",
          "comet-test2@dummy.com"
        )
      }
    }
  }
}
