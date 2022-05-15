package ai.starlake.workflow

import better.files.File
import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import org.apache.hadoop.fs.Path

import scala.io.Codec

class IngestionWorkflowSpec extends TestHelper {
  new WithSettings {

    private def loadLandingFile(landingFile: String): TestHelper#SpecTrait = {
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
        storageHandler.touchz(new Path(landingPath, "_SUCCESS"))

        loadLanding(Codec.default, createAckFile = false)
        val destFolder: Path = DatasetArea.pending(datasetDomainName)
        assert(
          storageHandler.exists(new Path(destFolder, "SCHEMA-VALID.dsv")),
          "Landing file directly imported"
        )
        assert(
          !storageHandler.exists(new Path(destFolder, "_SUCCESS")),
          "Unrelated file ignored"
        )
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
        assert(
          storageHandler.exists(new Path(destFolder, "SCHEMA-VALID.dsv")),
          "Landing file based on extension imported"
        )
        val ackFilename: String = File(landingFile).nameWithoutExtension + ".ack"
        assert(
          !storageHandler.exists(new Path(destFolder, ackFilename)),
          "Ack file not imported"
        )
        assert(
          !storageHandler.exists(new Path(landingPath, ackFilename)),
          "Ack file removed from landing zone"
        )
      }
    }

    "Loading files in Landing area" should "produce file in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv")
    }

    "Loading zip files in Landing area" should "produce file contained in Zip File in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv.zip")
    }

    "Loading gzip files in Landing area" should "produce file contained in GZ File in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv.gz")
    }

    "Loading tgz files in Landing area" should "produce file contained in tgz File in pending area" in {
      loadLandingFile("/sample/SCHEMA-VALID.dsv.tgz")
    }
  }
}
