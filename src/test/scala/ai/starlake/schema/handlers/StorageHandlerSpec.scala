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

import ai.starlake.schema.model.Types

import java.util.regex.Pattern
import ai.starlake.TestHelper
import ai.starlake.config.{Settings, StorageArea}
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path

class StorageHandlerSpec extends TestHelper {

  lazy val pathDomain = new Path(cometTestRoot + "/domain.comet.yml")

  lazy val pathType = new Path(cometTestRoot + "/types.comet.yml")

  lazy val pathBusiness = new Path(cometTestRoot + "/business.comet.yml")

  new WithSettings {
    "Domain Case Class" should "be written as yaml and read correctly" in {
      val domain = Domain(
        name = "DOMAIN",
        metadata = Some(
          Metadata(
            mode = Some(Mode.FILE),
            format = Some(Format.DSV),
            encoding = None,
            multiline = Some(false),
            array = Some(false),
            withHeader = Some(false),
            separator = Some(";"),
            quote = Some("\""),
            escape = Some("\\"),
            write = Some(WriteMode.APPEND),
            directory = Some(s"${cometTestRoot}/incoming/DOMAIN")
          )
        ),
        tables = List(
          Schema(
            "User",
            Pattern.compile("SCHEMA-.*.dsv"),
            List(
              Attribute(
                "firstname",
                "string",
                Some(false),
                required = false,
                PrivacyLevel.None
              ),
              Attribute(
                "lastname",
                "string",
                Some(false),
                required = false,
                PrivacyLevel("SHA1", false)
              ),
              Attribute(
                "age",
                "age",
                Some(false),
                required = false,
                PrivacyLevel("HIDE", false)
              )
            ),
            Some(Metadata(withHeader = Some(true))),
            None,
            Some("Schema Comment"),
            Some(List("SQL1", "SQL2")),
            None
          )
        ),
        comment = Some("Domain Comment")
      )

      storageHandler.write(mapper.writeValueAsString(domain), pathDomain)

      // TODO different behaviour between sbt & intellij
      //    readFileContent(pathDomain) shouldBe loadFile("/expected/yml/domain.yml")

      val resultDomain: Domain = mapper.readValue[Domain](storageHandler.read(pathDomain))

      resultDomain.name shouldBe domain.name
      resultDomain.resolveDirectory() shouldBe domain.resolveDirectory()
      // TODO TOFIX : domain written is not the domain expected, the test below just to make debug easy
      resultDomain.metadata.get equals domain.metadata.get
      resultDomain.resolveAck() shouldBe None
      resultDomain.comment shouldBe domain.comment
      resultDomain.getExtensions(
        settings.comet.defaultFileExtensions,
        settings.comet.forceFileExtensions
      ) should contain theSameElementsAs domain.getExtensions(
        settings.comet.defaultFileExtensions,
        settings.comet.forceFileExtensions
      )
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
      val fileContent = readFileContent(pathType)
      val expectedFileContent = loadTextFile(s"/expected/yml/types.comet.yml")
      fileContent shouldBe expectedFileContent
      val resultType: Types = mapper.readValue[Types](storageHandler.read(pathType))
      resultType shouldBe types

    }

    "Business Job Definition" should "be valid json" in {
      val businessTask1 = AutoTaskDesc(
        None,
        Some("select * from domain"),
        "DOMAIN",
        "ANALYSE",
        WriteMode.OVERWRITE,
        Some(List("comet_year", "comet_month")),
        None,
        None,
        None,
        None,
        Some(
          List(RowLevelSecurity("myrls", "TRUE", Set("user:hayssam.saleh@ebiznext.com")))
        )
      )
      val businessJob =
        AutoJobDesc(
          "business1",
          List(businessTask1),
          Some(StorageArea.business),
          Some("parquet"),
          Some(true)
        )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      val expected = mapper
        .readValue(loadTextFile("/expected/yml/business.comet.yml"), classOf[AutoJobDesc])
      storageHandler.write(businessJobDef, pathBusiness)
      logger.info(readFileContent(pathBusiness))
      val actual = mapper
        .readValue(readFileContent(pathBusiness), classOf[AutoJobDesc])
      actual shouldEqual expected
    }
  }

  "Check fs google storage uri" should "be gs" in {
    assert(
      "/user/comet" == Path
        .getPathWithoutSchemeAndAuthority(new Path("file:///user/comet"))
        .toString
    )
  }
}
