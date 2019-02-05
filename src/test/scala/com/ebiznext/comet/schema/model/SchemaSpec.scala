package com.ebiznext.comet.schema.model

import java.io.InputStream

import com.ebiznext.comet.TestHelper
import org.scalatest.{FlatSpec, Matchers}

class SchemaSpec extends FlatSpec with Matchers with TestHelper {

  "Attribute type" should "be valid" in {
    val stream: InputStream = getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    val attr = Attribute("attr",
      "invalid-type", // should raise error non existent type
      Some(true),
      true,
      Some(PrivacyLevel.MD5) // Should raise an error. Privacy cannot be applied on types other than string
    )

    attr.checkValidity(types.types) shouldBe Left(List("Invalid Type invalid-type"))
  }

  "Attribute privacy" should "be applied on string type only" in {
    val stream: InputStream = getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    val attr = Attribute("attr",
      "long",
      Some(true),
      true,
      Some(PrivacyLevel.MD5) // Should raise an error. Privacy cannot be applied on types other than string
    )
    attr.checkValidity(types.types) shouldBe
      Left(
        List("Attribute Attribute(attr,long,Some(true),true,Some(MD5),None,None,None,None) : string is the only supported primitive type for an attribute when privacy is requested")
      )
  }

  "Sub Attribute" should "be present for struct types only" in {
    val stream: InputStream = getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    val attr = Attribute("attr",
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
}
