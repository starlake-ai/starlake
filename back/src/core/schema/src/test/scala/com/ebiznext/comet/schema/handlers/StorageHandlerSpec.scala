package com.ebiznext.comet.schema.handlers

import java.io.File

import com.ebiznext.comet.data.Data
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Types}
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}
import org.json4s.native.Serialization.{read => jsread, write => jswrite}

class StorageHandlerSpec extends FlatSpec with Matchers with Data {
  implicit val formats = SchemaModel.formats

  "Domain Case Class" should "be written as json" in {
    val path = new Path("/tmp/domain.json")
    val sh = new HdfsStorageHandler
    sh.write(jswrite(domain), path)
  }

  "json Domain" should "be read into a case class" in {
    val path = new Path("/tmp/domain.json")
    val sh = new HdfsStorageHandler
    val ldomain = jsread[Domain](sh.read(path))
    // assert(ldomain == domain)
  }

  "Types Case Class" should "be written as json" in {
    val path = new Path("/tmp/types.json")
    val sh = new HdfsStorageHandler
    sh.write(jswrite(types), path)
  }

  "json Types" should "be read into a case class" in {
    val path = new Path("/tmp/types.json")
    val sh = new HdfsStorageHandler
    val ltypes = jsread[Types](sh.read(path))
    //assert(ltypes == types)
  }
}
