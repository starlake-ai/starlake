package com.ebiznext.comet.schema.model

import java.io.InputStream

import com.ebiznext.comet.sample.SampleData
import org.scalatest.{FlatSpec, Matchers}

class TypesSpec extends FlatSpec with Matchers with SampleData {

  "Default types" should "be valid" in {
    val stream: InputStream = getClass.getResourceAsStream("/sample/metadata/types/default.yml")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val types = mapper.readValue(lines, classOf[Types])
    val res = types.checkValidity()
    res match {
      case Left(errors) =>
        errors.foreach(println)
        assert(false)
      case Right(_) =>
        assert(true)
    }
  }

  "Duplicate  type names" should "be refused" in {
    val stream: InputStream = getClass.getResourceAsStream("/sample/metadata/types/default.yml")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n") +
      """
        |  - name: "long"
        |    primitiveType: "long"
        |    pattern: "-?\\d+"
        |    sample: "-64564"
      """.stripMargin
    println(lines)
    val types = mapper.readValue(lines, classOf[Types])
    val res = types.checkValidity()
    res match {
      case Left(errors) =>
        errors.foreach(println)
        assert(true)
      case Right(_) =>
        assert(false)
    }
  }

  //  "json case object" should "deserialize as case olass" in {
  //    val jsdomain = mapper.readValue(domainStr, classOf[Domain])
  //    assert(jsdomain == domain)
  //  }

}
