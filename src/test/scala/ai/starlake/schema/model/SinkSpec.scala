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

package ai.starlake.schema.model

import ai.starlake.TestHelper

class SinkSpec extends TestHelper {
  new WithSettings {
    "parsing any sink without options" should "succeed" in {
      mapper.readValue(
        """
          |name: "sink"
          |type: "None"
          |""".stripMargin,
        classOf[Sink]
      ) shouldBe NoneSink(name = Some("sink"))
    }

    "parsing any sink with options" should "succeed" in {
      mapper.readValue(
        """
          |name: "sink"
          |type: "None"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
        classOf[Sink]
      ) shouldBe NoneSink(
        name = Some("sink"),
        options = Some(Map("anyOption" -> "true"))
      )
    }

    "writing any sink without options" should "succeed" in {
      mapper.writeValueAsString(
        NoneSink(name = Some("sink"))
      ) shouldBe """--- !<None>
                         |name: "sink"
                         |type: "None"
                         |""".stripMargin
    }

    "writing any sink with options" should "succeed" in {
      mapper.writeValueAsString(
        NoneSink(
          name = Some("sink"),
          options = Some(Map("anyOption" -> "true"))
        )
      ) shouldBe """--- !<None>
                         |name: "sink"
                         |options:
                         |  anyOption: "true"
                         |type: "None"
                         |""".stripMargin
    }

    "parsing BQ sink" should "succeed" in {
      mapper.readValue(
        """
          |name: "sink"
          |type: "BQ"
          |timestamp: "timestamp"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
        classOf[Sink]
      ) shouldBe BigQuerySink(
        name = Some("sink"),
        timestamp = Some("timestamp"),
        options = Some(Map("anyOption" -> "true"))
      )
    }

    "parsing FS sink" should "succeed" in {
      mapper.readValue(
        """
          |name: "sink"
          |type: "FS"
          |extension: "extension"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
        classOf[Sink]
      ) shouldBe FsSink(
        name = Some("sink"),
        extension = Some("extension"),
        options = Some(Map("anyOption" -> "true"))
      )
    }

    "parsing ES sink" should "succeed" in {
      mapper.readValue(
        """
          |name: "sink"
          |type: "ES"
          |timestamp: "timestamp"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
        classOf[Sink]
      ) shouldBe EsSink(
        name = Some("sink"),
        timestamp = Some("timestamp"),
        options = Some(Map("anyOption" -> "true"))
      )
    }

    "parsing JDBC sink" should "succeed" in {
      mapper.readValue(
        """
          |name: "sink"
          |type: "JDBC"
          |connection: "connection"
          |partitions: 3
          |options:
          |  anyOption: "true"
          |""".stripMargin,
        classOf[Sink]
      ) shouldBe JdbcSink(
        name = Some("sink"),
        connection = "connection",
        partitions = Some(3),
        options = Some(Map("anyOption" -> "true"))
      )
    }

    "parsing KAFKA sink" should "fail" in {
      assertThrows[Exception] {
        mapper.readValue(
          """
          |name: "sink"
          |type: "KAFKA"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
          classOf[Sink]
        )
      }
    }
  }
}
