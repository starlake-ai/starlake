package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path

class AutoTaskSpec extends TestHelper {
  new WithSettings() {
    "Load Transform Job with taskrefs" should "succeed" in {
      new SpecTrait(
        domainOrJobFilename = "csvOutputJob.comet.yml",
        sourceDomainOrJobPathname = "/sample/job/csvOutputJob.comet.yml",
        datasetDomainName = "file",
        sourceDatasetPathName = "",
        isDomain = false
      ) {
        cleanMetadata
        cleanDatasets
        val schemaHandler = new SchemaHandler(settings.storageHandler)
        val workflow = new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
        workflow.autoJob(TransformConfig(name = "csvOutputJob"))

        readFileContent(
          new Path(cometDatasetsPath + "/result/file/file.csv")
        ) shouldBe "  Name|Last Name   |"
      }
    }
  }
}
