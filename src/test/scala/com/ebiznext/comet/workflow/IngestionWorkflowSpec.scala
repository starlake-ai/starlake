package com.ebiznext.comet.workflow

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import org.apache.hadoop.fs.Path

class IngestionWorkflowSpec extends TestHelper {
  new WithSettings() {

    private def loadLandingFile(landingFile: String, pendingFile: String) = {
      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = "/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = landingFile
      ) {
        cleanMetadata
        cleanDatasets

        storageHandler.delete(new Path(landingPath))
        // Make sure unrelated files, even without extensions, are not imported
        withSettings.deliverTestFile("/sample/_SUCCESS", new Path(landingPath, "_SUCCESS"))

        loadLanding
        val destFolder: Path = DatasetArea.pending(datasetDomainName)
        assert(storageHandler.exists(new Path(destFolder, pendingFile)), "Landing file directly imported")
        assert(!storageHandler.exists(new Path(destFolder, "_SUCCESS")), "Unrelated file ignored")
      }

      // Test again, but with Domain.ack defined
      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = "/sample/DOMAIN-ACK.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = landingFile
      ) {
        cleanMetadata
        cleanDatasets

        storageHandler.delete(new Path(landingPath))

        loadLanding
        val destFolder: Path = DatasetArea.pending(datasetDomainName)
        assert(storageHandler.exists(new Path(destFolder, pendingFile)), "Landing file based on extension imported")
      }
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
