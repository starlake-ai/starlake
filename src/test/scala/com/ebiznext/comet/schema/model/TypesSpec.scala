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

package com.ebiznext.comet.schema.model

import java.io.InputStream

import com.ebiznext.comet.TestHelper
import org.scalatest.{FlatSpec, Matchers}

class TypesSpec extends TestHelper {

  "Default types" should "be valid" in {
    val stream: InputStream =
      getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    types.checkValidity() shouldBe Right(true)
  }

  "Duplicate  type names" should "be refused" in {
    val stream: InputStream =
      getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines = scala.io.Source
      .fromInputStream(stream)
      .getLines()
      .mkString("\n") +
    """
        |  - name: "long"
        |    primitiveType: "long"
        |    pattern: "-?\\d+"
        |    sample: "-64564"
      """.stripMargin

    val types = mapper.readValue(lines, classOf[Types])
    types.checkValidity() shouldBe Left(
      List("long is defined 2 times. A type can only be defined once.")
    )

  }

  "Date / Time Pattern" should "be valid" in {
    val stream: InputStream =
      getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines = scala.io.Source
      .fromInputStream(stream)
      .getLines()
      .mkString("\n") +
    """
        |  - name: "timeinmillis"
        |    primitiveType: "timestamp"
        |    pattern: "epoch_milli"
        |    sample: "1548923449662"
        |  - name: "timeinseconds"
        |    primitiveType: "timestamp"
        |    pattern: "epoch_second"
        |    sample: "1548923449"
      """.stripMargin
    val types = mapper.readValue(lines, classOf[Types])

    types.checkValidity() shouldBe Right(true)

    "2019-01-31 09:30:49.662" shouldBe types.types
      .find(_.name == "timeinmillis")
      .get
      .sparkValue("1548923449662")
      .toString
    "2019-01-31 09:30:49.0" shouldBe types.types
      .find(_.name == "timeinseconds")
      .get
      .sparkValue("1548923449")
      .toString

  }

}
