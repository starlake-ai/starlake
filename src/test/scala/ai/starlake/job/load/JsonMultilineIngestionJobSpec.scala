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

package ai.starlake.job.load

import ai.starlake.{JdbcChecks, TestHelper}

class JsonMultilineIngestionJobSpec extends TestHelper with JdbcChecks {

  "Ingest Complex Multiline JSON " should "should be ingested from pending to accepted, and archived " in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/jsonmultiline/json-multiline.sl.yml",
        datasetDomainName = "jsonmultiline",
        sourceDatasetPathName = "/sample/jsonmultiline/complex-multiline.json"
      ) {
        Thread.sleep(5000)
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable("/sample/jsonmultiline/sample_json.sl.yml")
        loadPending

        val schemaHandler = settings.schemaHandler()
        val schema = schemaHandler.table("jsonmultiline", "sample_json").get
        val sparkSchema = schema.sourceSparkSchemaWithoutScriptedFields(schemaHandler)

        // Accepted should have the same data as input
        val location = getTablePath(datasetDomainName, "sample_json")

        val resultDf = sparkSession.read
          .format(settings.appConfig.defaultWriteFormat)
          .load(location)
          .where(getTodayCondition)

        logger.info(resultDf.showString(truncate = 0))
        private val session = sparkSession
        import session.implicits._
        private val expected = List(
          "comet-test@dummy.com",
          "comet-test2@dummy.com"
        )
        resultDf
          .select($"email")
          .map(_.getString(0))
          .collect() should contain theSameElementsAs expected
      }
    }
  }
}
