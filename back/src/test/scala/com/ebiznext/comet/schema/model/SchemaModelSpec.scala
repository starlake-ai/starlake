package com.ebiznext.comet.schema.model

import com.ebiznext.comet.sample.SampleData
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest._

case class XMode(mode: Mode)

class SchemaModelSpec extends FlatSpec with Matchers with SampleData {

  "Case Object" should "serialize as a simple string" in {
    println(mapper.writeValueAsString(domain))
    assert(1 == 1)
  }

  //  "json case object" should "deserialize as case olass" in {
  //    val jsdomain = mapper.readValue(domainStr, classOf[Domain])
  //    assert(jsdomain == domain)
  //  }

}
