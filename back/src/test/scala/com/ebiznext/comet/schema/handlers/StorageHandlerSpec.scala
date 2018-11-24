package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.data.Data
import com.ebiznext.comet.schema.model.SchemaModel._
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class StorageHandlerSpec extends FlatSpec with Matchers with Data {
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  // provides all of the Scala goodiness
  mapper.registerModule(DefaultScalaModule)

  "Domain Case Class" should "be written as yaml" in {
    val path = new Path("/tmp/domain.yml")
    val sh = new HdfsStorageHandler

    sh.write(mapper.writeValueAsString(domain), path)
  }

  "yaml Domain" should "be read into a case class" in {
    val path = new Path("/tmp/domain.yml")
    val sh = new HdfsStorageHandler

    val ldomain = mapper.readValue(sh.read(path), classOf[Domain])
    assert(ldomain == domain)
  }

  "Types Case Class" should "be written as yaml" in {
    val path = new Path("/tmp/types.yml")
    val sh = new HdfsStorageHandler
    sh.write(mapper.writeValueAsString(types), path)
  }

  "yaml Types" should "be read into a case class" in {
    val path = new Path("/tmp/types.yml")
    val sh = new HdfsStorageHandler
    val ltypes = mapper.readValue(sh.read(path), classOf[Types])
    assert(ltypes == types)
  }

  "Business Job Definition" should "be valid json" in {
    /*
    case class BusinessTask(sql: String, domain: String, dataset: String, write: Write)

    case class BusinessJob(name: String, cron: String, tasks: Array[BusinessTask])
     */

    val businessTask1 = BusinessTask("select * from domain", "DOMAIN", "ANALYSE", Write.OVERWRITE)
    val businessJob = BusinessJob("business1", "0 * * * *", List(businessTask1))
    val sh = new HdfsStorageHandler
    val path = new Path("/tmp/business.yml")
    sh.write(mapper.writeValueAsString(businessJob), path)
  }

}
