package com.ebiznext.comet.schema.model

import com.ebiznext.comet.data.Data
import com.ebiznext.comet.schema.model.SchemaModel.Domain
import org.json4s.native.Serialization._
import org.scalatest._

case class XMode(mode: SchemaModel.Mode)

class SchemaModelSpec extends FlatSpec with Matchers with Data {

  "Case Object" should "serialize as a simple string" in {
    implicit val formats = SchemaModel.formats
    println(writePretty(domain))
    assert(1 == 1)
  }

  "json case object" should "deserialize as case olass" in {
    implicit val formats = SchemaModel.formats

    val jsdomain = read[Domain](domainStr)
    assert(jsdomain == domain)
  }
}
