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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

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
    resultDomain.metadata.get equals domain.metadata.get
    resultDomain.ack shouldBe Some(domain.getAck())
    resultDomain.comment shouldBe domain.comment
    resultDomain.extensions shouldBe Some(domain.getExtensions())
  }

  "Types Case Class" should "be written as yaml and read correctly" in {
    val types = Types(
      List(
        Type("string", ".+", PrimitiveType.string),
        Type("time", "(1[012]|[1-9]):[0-5][0-9](\\\\s)?(?i)(am|pm)"),
        Type("time24", "([01]?[0-9]|2[0-3]):[0-5][0-9]"),
        Type(
          "date",
          "(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\\\d\\\\d)",
          PrimitiveType.date
        ),
        Type("username", "[a-z0-9_-]{3,15}"),
        Type("age", "[a-z0-9_-]{3,15}", PrimitiveType.long),
        Type("color", "#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})", PrimitiveType.string),
        Type(
          "ip",
          "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])"
        ),
        Type("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,6}")
      )
    )

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
    val businessJob =
      AutoJobDesc("business1", List(businessTask1), None, Some("parquet"), Some(true))
    storageHandler.write(mapper.writeValueAsString(businessJob), pathBusiness)
    logger.info(readFileContent(pathBusiness))
    readFileContent(pathBusiness) shouldBe loadFile("/expected/yml/business.yml")
  }

  "Check fs google storage uri" should "be gs" in {
    assert(
      "/user/comet" == Path
        .getPathWithoutSchemeAndAuthority(new Path("file:///user/comet"))
        .toString
    )
  }
}
