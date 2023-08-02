package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{AutoJobDesc, BigQuerySink, WriteMode}
import ai.starlake.utils.YamlSerializer
import better.files.File
import com.typesafe.config.{Config, ConfigFactory}

class Xls2YmlAutoJobTest extends TestHelper {

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
    val outputFile = File(DatasetArea.jobs.toString + "/someJob.comet.yml")

    println(outputFile.contentAsString)
    val result: AutoJobDesc = YamlSerializer
      .deserializeJob(outputFile.contentAsString, outputFile.pathAsString)
      .getOrElse(throw new Exception(s"Invalid file name $outputFile"))

    "Parsing a sample xlsx file" should "generate a yml file sqlEngine BQ" in {
      outputFile.exists() shouldBe true
      result.name shouldBe "someJob"
      result.tasks.size shouldBe 1
    }

    it should "have table specification sqlEngine BQ" in {
      val job = result.tasks.head

      job.domain shouldBe "someDomain"
      job.table shouldBe "dataset"
      job.write shouldBe WriteMode.OVERWRITE
      job.sink.map(_.getSink()) shouldBe Some(
        BigQuerySink(
          timestamp = Some("partitionCol"),
          requirePartitionFilter = Some(true)
        )
      )
      job.comment shouldBe Some("jointure source1 et source2")
      job.rls.size shouldBe 0
      job.acl.size shouldBe 1
      job.attributesDesc.size shouldBe 3
      job.attributesDesc.map(_.comment) shouldEqual List(
        "description colonne 1",
        "description colonne 2",
        "date de traitement"
      )
    }

    val outputFile2 = File(DatasetArea.jobs.toString + "/someJob2.comet.yml")

    val result2: AutoJobDesc = YamlSerializer
      .deserializeJob(outputFile2.contentAsString, outputFile2.pathAsString)
      .getOrElse(throw new Exception(s"Invalid file name $outputFile2"))

    "Parsing a sample xlsx file" should "generate a yml file sqlEngine BQ 2" in {
      outputFile2.exists() shouldBe true
      result2.name shouldBe "someJob2"
      result2.tasks.size shouldBe 1
    }

    Xls2YmlAutoJob.generateSchema(
      getClass.getResource("/sample/SomeJobTemplateBQ.xls").getPath,
      Some(getClass.getResource("/sample/SomePolicies.xls").getPath)
    )
    val outputFileBQ = File(DatasetArea.jobs.toString + "/someJobBQ.comet.yml")

    val resultBQ: AutoJobDesc = YamlSerializer
      .deserializeJob(outputFileBQ.contentAsString, outputFileBQ.pathAsString)
      .getOrElse(throw new Exception(s"Invalid file name $outputFileBQ"))

    "Parsing a sample xlsx file" should "generate a yml file engine bq" in {
      outputFileBQ.exists() shouldBe true
      resultBQ.name shouldBe "someJobBQ"
      resultBQ.tasks.size shouldBe 1
    }

    it should "have table specification engine bq" in {
      val job = resultBQ.tasks.head

      job.domain shouldBe "someDomain"
      job.table shouldBe "dataset"
      job.write shouldBe WriteMode.OVERWRITE
      job.sink.map(_.getSink()) shouldBe Some(BigQuerySink())
      job.comment shouldBe Some("jointure source1 et source2")
      job.rls.size shouldBe 0
      job.acl.size shouldBe 0
      job.attributesDesc.size shouldBe 2
      job.attributesDesc.map(_.comment) shouldEqual List(
        "description colonne 1",
        "description colonne 2"
      )
    }
  }
}
