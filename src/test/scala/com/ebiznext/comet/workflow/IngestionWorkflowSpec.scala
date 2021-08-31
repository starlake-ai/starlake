package com.ebiznext.comet.workflow

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import org.apache.hadoop.fs.Path

class IngestionWorkflowSpec extends TestHelper {
  new WithSettings() {

    private def loadLandingFile(landingFile: String, pendingFile: String) = {
      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = s"/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = landingFile
      ) {
        cleanMetadata
        cleanDatasets

        loadLanding
        val destFolder = DatasetArea.pending(datasetDomainName)
        val targetFile = new Path(
          destFolder,
          pendingFile
        )
        assert(storageHandler.exists(targetFile))
      }
    }

    "Loading files without extension in Landing area" should "produce file in pending area" in {
      loadLandingFile("/sample/_SUCCESS", "_SUCCESS")
    }

    "Loading files in Landing area" should "produce file in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv", "SCHEMA-VALID.dsv")
    }

    "Loading zip files in Landing area" should "produce file contained in Zip File in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv.zip", "SCHEMA-VALID.dsv")
    }

    "Loading gzip files in Landing area" should "produce file contained in GZ File in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv.gz", "SCHEMA-VALID.dsv")
    }

    "Loading tgz files in Landing area" should "produce file contained in tgz File in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv.tgz", "SCHEMA-VALID.dsv")
    }
  }
}
