package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path

class AutoTaskSpec extends TestHelper {
  new WithSettings() {
    "File Sink Spark Options in SQL job description " should "be applied to resulting file" in {
      new SpecTrait(
        jobFilename = Some("_config.sl.yml"),
        sourceDomainOrJobPathname = "/sample/job/sql/_config.sl.yml",
        datasetDomainName = "file",
        sourceDatasetPathName = ""
      ) {
        cleanMetadata
        cleanDatasets
        val schemaHandler = new SchemaHandler(settings.storageHandler())
        val workflow = new IngestionWorkflow(storageHandler, schemaHandler)
        workflow.autoJob(TransformConfig(name = "result.file"))

        readFileContent(
          new Path(starlakeDatasetsPath + "/business/result/file/file.csv")
        ) shouldBe "  Name|Last Name   |"
      }
    }
    "File Sink Spark Options in Python job description " should "be applied to resulting file" in {
      // TODO test pyspark code from scala unit tests
      // This test is currently run locally as part of the local sample.
      if (false) {
        new SpecTrait(
          jobFilename = Some("piJob.sl.yml"),
          sourceDomainOrJobPathname = "/sample/job/python/piJob.sl.yml",
          datasetDomainName = "file",
          sourceDatasetPathName = ""
        ) {
          cleanMetadata
          cleanDatasets
          deliverTestFile(
            "/sample/job/python/piJob.pi.py",
            new Path(this.jobMetadataRootPath, "piJob.pi.py")
          )
          val schemaHandler = new SchemaHandler(settings.storageHandler())
          val workflow = new IngestionWorkflow(storageHandler, schemaHandler)
          workflow.autoJob(TransformConfig(name = "piJob"))

          readFileContent(
            new Path(starlakeDatasetsPath + "/business/result/file/file.csv")
          ).trim shouldBe "Pi is roughly 3.137320"
        }
      }
    }
  }
}
