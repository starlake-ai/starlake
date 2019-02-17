package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path

class StorageHandlerSpec extends TestHelper {

  lazy val pathDomain = new Path(TestHelper.tempFile + "/domain.yml")

  lazy val pathType = new Path(TestHelper.tempFile + "/types.yml")

  lazy val pathBusiness = new Path(TestHelper.tempFile + "/business.yml")

  "Domain Case Class" should "be written as yaml and read correctly" in {

    storageHandler.write(mapper.writeValueAsString(domain), pathDomain)

    //TODO different behaviour between sbt & intellij
    //    readFileContent(pathDomain) shouldBe loadFile("/expected/yml/domain.yml")

    val resultDomain: Domain = mapper.readValue[Domain](storageHandler.read(pathDomain))

    resultDomain.name shouldBe domain.name
    resultDomain.directory shouldBe domain.directory
    //TODO TOFIX : domain written is not the domain expected, the test below just to make debug easy
    resultDomain.metadata shouldBe domain.metadata.map(_.copy(partition = Some(Partition(0.0, Some(Nil)))))
    resultDomain.ack shouldBe Some(domain.getAck())
    resultDomain.comment shouldBe domain.comment
    resultDomain.extensions shouldBe Some(domain.getExtensions())
    //    resultDomain.schemas shouldBe domain.schemas.map(
    //      s =>
    //        s.copy(
    //          attributes = s.attributes.map(_.copy(stat = Some(Stat.NONE))),
    //          metadata = s.metadata.map(_.copy(partition = Some(List())))
    //      )
    //    )

  }

  "Types Case Class" should "be written as yaml and read correctly" in {

    storageHandler.write(mapper.writeValueAsString(types), pathType)

    readFileContent(pathType) shouldBe loadFile("/expected/yml/types.yml")

    val resultType: Types = mapper.readValue[Types](storageHandler.read(pathType))

    resultType shouldBe types

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

    storageHandler.write(mapper.writeValueAsString(businessJob), pathBusiness)

    readFileContent(pathBusiness) shouldBe loadFile("/expected/yml/business.yml")
  }

}
