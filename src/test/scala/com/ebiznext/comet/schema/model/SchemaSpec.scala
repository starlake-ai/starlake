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

import java.io.{InputStream, StringWriter}

import com.ebiznext.comet.TestHelper
import org.scalatest.{FlatSpec, Matchers}

class SchemaSpec extends FlatSpec with Matchers with TestHelper {

  "Attribute type" should "be valid" in {
    val stream: InputStream =
      getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    val attr = Attribute(
      "attr",
      "invalid-type", // should raise error non existent type
      Some(true),
      true,
      Some(PrivacyLevel.MD5) // Should raise an error. Privacy cannot be applied on types other than string
    )

    attr.checkValidity(types.types) shouldBe Left(List("Invalid Type invalid-type"))
  }

  "Attribute privacy" should "be applied on string type only" in {
    val stream: InputStream =
      getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    val attr = Attribute(
      "attr",
      "long",
      Some(true),
      true,
      Some(PrivacyLevel.MD5) // Should raise an error. Privacy cannot be applied on types other than string
    )
    attr.checkValidity(types.types) shouldBe
      Left(
        List(
          "Attribute Attribute(attr,long,Some(true),true,Some(MD5),None,None,None,None) : string is the only supported primitive type for an attribute when privacy is requested"
        )
      )
  }

  "Sub Attribute" should "be present for struct types only" in {
    val stream: InputStream =
      getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    val attr = Attribute(
      "attr",
      "long",
      Some(true),
      true,
      Some(PrivacyLevel.MD5), // Should raise an error. Privacy cannot be applied on types other than string
      attributes = Some(List[Attribute]())
    )
    val expectedErrors = List(
      "Attribute Attribute(attr,long,Some(true),true,Some(MD5),None,None,None,Some(List())) : string is the only supported primitive type for an attribute when privacy is requested",
      "Attribute Attribute(attr,long,Some(true),true,Some(MD5),None,None,None,Some(List())) : Simple attributes cannot have sub-attributes",
      "Attribute Attribute(attr,long,Some(true),true,Some(MD5),None,None,None,Some(List())) : when present, attributes list cannot be empty."
    )

    attr.checkValidity(types.types) shouldBe Left(expectedErrors)
  }

  "Position serialization" should "output all fields" in {
    val yml =
      """---
        |name: "hello"
        |type: "string"
        |array: false
        |required: true
        |privacy: "NONE"
        |comment: null
        |rename: null
        |metricType: null
        |attributes: null
        |position:
        |  first: 1
        |  last: 2
        |  ltrim: true
        |  rtrim: true""".stripMargin

    val attr = Attribute("hello", position = Some(Position(1, 2)))
    val writer = new StringWriter()
    mapper.writer().writeValue(writer, attr)
    writer.toString.trim should equal(yml)
  }

}
