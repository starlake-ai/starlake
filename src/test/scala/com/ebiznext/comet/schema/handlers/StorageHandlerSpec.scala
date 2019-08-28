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

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path

class StorageHandlerSpec extends TestHelper {

  lazy val pathDomain = new Path(TestHelper.tempFile + "/domain.yml")

  lazy val pathType = new Path(TestHelper.tempFile + "/types.yml")

  lazy val pathBusiness = new Path(TestHelper.tempFile + "/business.yml")

  "Domain Case Class" should "be written as yaml and read correctly" in {

    storageHandler.write(mapper.writeValueAsString(domain), pathDomain)

    //TODO different behaviour between sbt & intellij
    //    readFileContent(pathDomain) shouldBe loadFile("/expected/yml/domain.yml")

    val resultDomain: Domain = mapper.readValue[Domain](storageHandler.read(pathDomain))

    resultDomain.name shouldBe domain.name
    resultDomain.directory shouldBe domain.directory
    //TODO TOFIX : domain written is not the domain expected, the test below just to make debug easy
    resultDomain.metadata shouldBe domain.metadata
    resultDomain.ack shouldBe Some(domain.getAck())
    resultDomain.comment shouldBe domain.comment
    resultDomain.extensions shouldBe Some(domain.getExtensions())
  }

  "Types Case Class" should "be written as yaml and read correctly" in {

    storageHandler.write(mapper.writeValueAsString(types), pathType)
    readFileContent(pathType) shouldBe loadFile("/expected/yml/types.yml")
    val resultType: Types = mapper.readValue[Types](storageHandler.read(pathType))
    resultType shouldBe types

  }

  "Business Job Definition" should "be valid json" in {
    val businessTask1 = AutoTaskDesc(
      "select * from domain",
      "DOMAIN",
      "ANALYSE",
      WriteMode.OVERWRITE,
      Some(List("comet_year", "comet_month"))
    )
    val businessJob = AutoJobDesc("business1", List(businessTask1))
    storageHandler.write(mapper.writeValueAsString(businessJob), pathBusiness)
    println(readFileContent(pathBusiness))
    readFileContent(pathBusiness) shouldBe loadFile("/expected/yml/business.yml")
  }
}


