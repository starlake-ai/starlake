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

import java.io.InputStream

import ai.starlake.TestHelper

class TypesSpec extends TestHelper {
  new WithSettings {
    "Default types" should "be valid" in {
      val stream: InputStream =
        getClass.getResourceAsStream("/quickstart/metadata/types/default.comet.yml")
      val lines =
        scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
      val types = mapper.readValue(lines, classOf[Types])
      types.checkValidity() shouldBe Right(true)
    }

    "Duplicate  type names" should "be refused" in {
      val stream: InputStream =
        getClass.getResourceAsStream("/quickstart/metadata/types/default.comet.yml")
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

    "Money Zone" should "be valid" in {
      val stream: InputStream =
        getClass.getResourceAsStream("/quickstart/metadata/types/default.comet.yml")
      val lines = scala.io.Source
        .fromInputStream(stream)
        .getLines()
        .mkString("\n") +
        """
          |  - name: "frenchdouble"
          |    primitiveType: "double"
          |    pattern: "-?\\d*,{0,1}\\d+"
          |    zone: fr_FR
          |    sample: "-64564,21"
      """.stripMargin
      val types = mapper.readValue(lines, classOf[Types])
      "-123.45" shouldBe types.types
        .find(_.name == "double")
        .get
        .sparkValue("-123.45")
        .toString

      "-123.45" shouldBe types.types
        .find(_.name == "frenchdouble")
        .get
        .sparkValue("-123,45")
        .toString

    }

    "Date / Time Pattern" should "be valid" in {
      val stream: InputStream =
        getClass.getResourceAsStream("/quickstart/metadata/types/default.comet.yml")
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
          |  - name: "utcdate"
          |    primitiveType: "timestamp"
          |    pattern: "dd/MM/yyyy HH:mm:ss"
          |    zone: "UTC-06:00"
          |    sample: "12/02/2019 08:03:05"
          |  - name: "instant"
          |    primitiveType: "timestamp"
          |    pattern: "ISO_INSTANT"
          |    sample: "2021-05-20T09:30:39.000000Z"
          |    comment: "Iso instant"
          |      """.stripMargin
      val types = mapper.readValue(lines, classOf[Types])

      types.checkValidity() shouldBe Right(true)

      def localTime(dateString: String) = {
        val calendar = java.util.Calendar.getInstance
        val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("Europe/Paris"))
        calendar.setTime(sdf.parse(dateString))
        sdf.setTimeZone(java.util.TimeZone.getDefault)
        sdf.format(calendar.getTime())
      }

      val ok = types.types.find(_.name == "instant").get.matches("2012-04-23 20:25:43.511Z")

      localTime("2019-01-31 09:30:49.662") shouldBe types.types
        .find(_.name == "timeinmillis")
        .get
        .sparkValue("1548923449662")
        .toString
      localTime("2019-01-31 09:30:49.0") shouldBe types.types
        .find(_.name == "timeinseconds")
        .get
        .sparkValue("1548923449")
        .toString + "00"

      "2019-01-31 09:30:49.0" should not be types.types
        .find(_.name == "utcdate")
        .get
        .sparkValue("31/01/2019 09:30:49")
        .toString

    }

    "Double Type with a Zone" should "be able to parse a value with a + prefix" in {
      val stream: InputStream =
        getClass.getResourceAsStream("/quickstart/metadata/types/default.comet.yml")
      val lines = scala.io.Source
        .fromInputStream(stream)
        .getLines()
        .mkString("\n") +
        """
        |  - name: "signed_double_fr"
        |    primitiveType: "double"
        |    pattern: "[+,-]?\\d*\\,{0,1}\\d+"
        |    sample: "+45.78"
        |    zone: Fr_fr
        |    comment: "Floating value with a sign prefix and french decimal point"
        |""".stripMargin
      val types = mapper.readValue(lines, classOf[Types])
      val doubleType = types.types
        .find(_.name == "double")
        .get
      doubleType.matches("+3.14") shouldBe false
      val signedDoubleType = types.types
        .find(_.name == "signed_double_fr")
        .get
      signedDoubleType.matches("+3,14") shouldBe true
      signedDoubleType.sparkValue("+3,14") shouldBe 3.14d
      signedDoubleType.matches("3,14") shouldBe true
      signedDoubleType.sparkValue("3,14") shouldBe 3.14d
      signedDoubleType.matches("-3,14") shouldBe true
      signedDoubleType.sparkValue("-3,14") shouldBe -3.14d

    }
  }
}
