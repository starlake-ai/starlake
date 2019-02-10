package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.sample.SampleData
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}

class StorageHandlerSpec extends FlatSpec with Matchers with SampleData {

  "Domain Case Class" should "be written as yaml" in {
    val path = new Path("/tmp/domain.yml")
    val sh = new HdfsStorageHandler

    sh.write(mapper.writeValueAsString(domain), path)
  }

  "yaml Domain" should "be read into a case class" in {
    val path = new Path("/tmp/domain.yml")
    val sh = new HdfsStorageHandler

    val _ = mapper.readValue(sh.read(path), classOf[Domain])
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
    val businessTask1 = AutoTask(
      "select * from domain",
      "DOMAIN",
      "ANALYSE",
      WriteMode.OVERWRITE,
      Some(List("comet_year", "comet_month")),
      None,
      None
    )
    val businessJob = AutoJobDesc("business1", List(businessTask1))
    val sh = new HdfsStorageHandler
    val path = new Path("/tmp/business.yml")
    sh.write(mapper.writeValueAsString(businessJob), path)
  }
}
