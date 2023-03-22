package ai.starlake.schema.generator

import better.files.File
import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{AutoJobDesc, BigQuerySink, Engine, WriteMode}
import ai.starlake.utils.YamlSerializer

class Xls2YmlAutoJobTest extends TestHelper {
  new WithSettings() {
    Xls2YmlAutoJob.generateSchema(
      getClass.getResource("/sample/SomeJobTemplate.xls").getPath,
      Some(getClass.getResource("/sample/SomePolicies.xls").getPath)
    )
    val outputFile = File(DatasetArea.jobs.toString + "/someJob.comet.yml")

    val result: AutoJobDesc = YamlSerializer
      .deserializeJob(outputFile.contentAsString, outputFile.pathAsString)
      .getOrElse(throw new Exception(s"Invalid file name $outputFile"))

    "Parsing a sample xlsx file" should "generate a yml file sqlEngine BQ" in {
      outputFile.exists() shouldBe true
      result.name shouldBe "someJob"
      result.tasks.size shouldBe 1
      result.engine shouldBe Some(Engine.SPARK)
    }

    it should "have table specification sqlEngine BQ" in {
      val job = result.tasks.head

      job.domain shouldBe "someDomain"
      job.table shouldBe "dataset"
      job.write shouldBe WriteMode.OVERWRITE
      job.sink shouldBe Some(
        BigQuerySink(timestamp = Some("partitionCol"), requirePartitionFilter = Some(true))
      )
      job.sqlEngine shouldBe Some(Engine.BQ)
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
      result2.engine shouldBe Some(Engine.SPARK)
      result2.tasks.head.sqlEngine shouldBe Some(Engine.BQ)
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
      resultBQ.engine shouldBe Some(Engine.BQ)
    }

    it should "have table specification engine bq" in {
      val job = resultBQ.tasks.head

      job.domain shouldBe "someDomain"
      job.table shouldBe "dataset"
      job.write shouldBe WriteMode.OVERWRITE
      job.sink shouldBe Some(
        BigQuerySink()
      )
      job.sqlEngine shouldBe None
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
