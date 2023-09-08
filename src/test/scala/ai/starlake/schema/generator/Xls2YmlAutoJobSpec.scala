package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{AutoTaskDesc, BigQuerySink, WriteMode}
import ai.starlake.utils.YamlSerializer
import better.files.File
import com.typesafe.config.{Config, ConfigFactory}

class Xls2YmlAutoJobSpec extends TestHelper {

  val bqConfiguration: Config = {
    val config = ConfigFactory
      .parseString("""
          |connectionRef = "bigquery"
          |""".stripMargin)
    val result = config.withFallback(super.testConfiguration)
    result
  }

  new WithSettings(bqConfiguration) {
    Xls2YmlAutoJob.generateSchema(
      getClass.getResource("/sample/SomeJobTemplate.xls").getPath,
      Some(getClass.getResource("/sample/SomePolicies.xls").getPath)
    )

    val outputFile = File(DatasetArea.transform.toString + s"/someDomain/someJob.comet.yml")
    println(outputFile.contentAsString)
    val result: AutoTaskDesc = YamlSerializer.deserializeTask(outputFile.contentAsString)

    "Parsing a sample xlsx file" should "generate a yml file sqlEngine BQ" in {
      outputFile.exists() shouldBe true
      result.name shouldBe "someJob"
    }

    it should "have table specification sqlEngine BQ" in {
      result.domain shouldBe "someDomain"
      result.table shouldBe "dataset"
      result.write shouldBe Some(WriteMode.OVERWRITE)
      result.sink.map { sink =>
        sink.timestamp shouldBe Some("partitionCol")
        sink.requirePartitionFilter shouldBe Some(true)
      }
      result.comment shouldBe Some("jointure source1 et source2")
      result.rls.size shouldBe 0
      result.acl.size shouldBe 1
      result.attributesDesc.size shouldBe 3
      result.attributesDesc.map(_.comment) shouldEqual List(
        "description colonne 1",
        "description colonne 2",
        "date de traitement"
      )
    }

    val outputFile2 = File(DatasetArea.transform.toString + "/someDomain/someJob2.comet.yml")

    val result2 = YamlSerializer.deserializeTask(outputFile2.contentAsString)

    "Parsing a sample xlsx file" should "generate a yml file sqlEngine BQ 2" in {
      outputFile2.exists() shouldBe true
      result2.name shouldBe "someJob2"
    }

    Xls2YmlAutoJob.generateSchema(
      getClass.getResource("/sample/SomeJobTemplateBQ.xls").getPath,
      Some(getClass.getResource("/sample/SomePolicies.xls").getPath)
    )
    val outputFileBQ = File(DatasetArea.transform.toString + "/someDomain/someJobBQ.comet.yml")

    val resultBQ = YamlSerializer.deserializeTask(outputFileBQ.contentAsString)

    "Parsing a sample xlsx file" should "generate a yml file engine bq" in {
      outputFileBQ.exists() shouldBe true
      resultBQ.name shouldBe "someJobBQ"
    }

    it should "have table specification engine bq" in {
      resultBQ.domain shouldBe "someDomain"
      resultBQ.table shouldBe "dataset"
      resultBQ.write shouldBe Some(WriteMode.OVERWRITE)
      resultBQ.sink.map(_.getSink()) shouldBe Some(BigQuerySink())
      resultBQ.comment shouldBe Some("jointure source1 et source2")
      resultBQ.rls.size shouldBe 0
      resultBQ.acl.size shouldBe 0
      resultBQ.attributesDesc.size shouldBe 2
      resultBQ.attributesDesc.map(_.comment) shouldEqual List(
        "description colonne 1",
        "description colonne 2"
      )
    }
  }
}
