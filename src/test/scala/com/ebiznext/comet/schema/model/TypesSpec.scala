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

  //  "json case object" should "deserialize as case olass" in {
  //    val jsdomain = mapper.readValue(domainStr, classOf[Domain])
  //    assert(jsdomain == domain)
  //  }

}
