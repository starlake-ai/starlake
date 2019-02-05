package com.ebiznext.comet.schema.model

import java.io.InputStream

import com.ebiznext.comet.sample.SampleData
import org.scalatest.{FlatSpec, Matchers}

class TypesSpec extends FlatSpec with Matchers with SampleData {

  "Default types" should "be valid" in {
    val stream: InputStream = getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    println(lines)
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
    val stream: InputStream = getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
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
  "Date / Time Pattern" should "be valid" in {
    val stream: InputStream = getClass.getResourceAsStream("/quickstart/metadata/types/default.yml")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n") +
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
    println(lines)
    val types = mapper.readValue(lines, classOf[Types])
    assert("2019-01-31 09:30:49.662" == types.types.find(_.name == "timeinmillis").get.sparkValue("1548923449662").toString)
    assert("2019-01-31 09:30:49.0" == types.types.find(_.name == "timeinseconds").get.sparkValue("1548923449").toString)


    val res = types.checkValidity()
    res match {
      case Left(errors) =>
        errors.foreach(println)
        assert(false)
      case Right(_) =>
        assert(true)
    }
  }

  //  "json case object" should "deserialize as case olass" in {
  //    val jsdomain = mapper.readValue(domainStr, classOf[Domain])
  //    assert(jsdomain == domain)
  //  }

}
