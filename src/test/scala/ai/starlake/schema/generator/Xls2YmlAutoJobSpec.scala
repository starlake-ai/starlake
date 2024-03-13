package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{AutoTaskDesc, BigQuerySink, FsSink, WriteMode}
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

    val outputFile: File = File(DatasetArea.transform.toString + s"/someDomain/someJob.sl.yml")
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
        sink.partition shouldBe Some(List("partitionCol"))
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

    val outputFile2: File = File(DatasetArea.transform.toString + "/someDomain/someJob2.sl.yml")

    val result2: AutoTaskDesc = YamlSerializer.deserializeTask(outputFile2.contentAsString)

    "Parsing a sample xlsx file" should "generate a yml file sqlEngine BQ 2" in {
      outputFile2.exists() shouldBe true
      result2.name shouldBe "someJob2"
    }

    val outputFile3: File = File(DatasetArea.transform.toString + "/someDomain/someJob3.sl.yml")

    val result3: AutoTaskDesc = YamlSerializer.deserializeTask(outputFile3.contentAsString)

    "Parsing a sample xlsx file" should "generate a yml file sqlEngine FS" in {
      outputFile3.exists() shouldBe true
      result3.name shouldBe "someJob3"
    }

    it should "have table specification engine FS" in {
      result3.domain shouldBe "someDomain"
      result3.table shouldBe "dataset3"
      result3.write shouldBe Some(WriteMode.OVERWRITE)
      result3.sink.map(_.getSink()) shouldBe Some(
        FsSink(
          connectionRef = Some("spark"),
          partition = Some(List("partitionCol")),
          format = Some("csv"),
          extension = Some(".csv"),
          coalesce = Some(true),
          options = Some(Map("opt1" -> "val1", "opt2" -> "val2"))
        )
      )
    }

    Xls2YmlAutoJob.generateSchema(
      getClass.getResource("/sample/SomeJobTemplateBQ.xls").getPath,
      Some(getClass.getResource("/sample/SomePolicies.xls").getPath)
    )
    val outputFileBQ: File = File(DatasetArea.transform.toString + "/someDomain/someJobBQ.sl.yml")

    val resultBQ: AutoTaskDesc = YamlSerializer.deserializeTask(outputFileBQ.contentAsString)

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
