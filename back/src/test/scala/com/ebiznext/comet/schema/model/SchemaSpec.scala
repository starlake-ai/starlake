package com.ebiznext.comet.schema.model

import java.io.InputStream

import com.ebiznext.comet.sample.SampleData
import org.scalatest.{FlatSpec, Matchers}

class SchemaSpec extends FlatSpec with Matchers with SampleData {

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
    val errs = attr.checkValidity(types.types)
    assert(errs.isLeft)
    errs.left.get.foreach(println)
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
    val errs = attr.checkValidity(types.types)
    assert(errs.isLeft)
    errs.left.get.foreach(println)
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
    val errs = attr.checkValidity(types.types)
    assert(errs.isLeft)
    errs.left.get.foreach(println)
  }
}
