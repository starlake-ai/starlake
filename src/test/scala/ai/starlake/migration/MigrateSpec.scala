package ai.starlake.migration

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.utils.YamlSerde
import better.files.File

import java.util.UUID.randomUUID

class MigrateSpec extends IntegrationTestBase {

  def withProject(version: String)(testCode: File => Any) = {
    val projectVersionFolder = migrationDir / version
    if (projectVersionFolder.notExists) {
      throw new RuntimeException(s"Project folder ${projectVersionFolder.pathAsString} not found")
    }
    import java.nio.file.Files
    val tempDir = File(Files.createTempDirectory(randomUUID.toString))
    try {
      logger.info(s"Copying project into ${tempDir.pathAsString}")
      projectVersionFolder.copyTo(tempDir)
      testCode(tempDir)
    } finally tempDir.delete()
  }

  def checkMigration(slRoot: File) = {
    val migratedFiles = slRoot.listRecursively.filter(_.isRegularFile).toList
    migratedFiles.foreach { filePath =>
      logger.info(s"Asserting $filePath")
      val expectedPath = filePath.pathAsString.replace(slRoot.pathAsString, "")
      YamlSerde.mapper.readTree(filePath.newFileReader) shouldBe YamlSerde.mapper
        .readTree((migrationDir / s"expected$expectedPath").newFileReader)
    }
    val expectedRoot = migrationDir / "expected"
    val expectedFiles = expectedRoot.listRecursively.filter(_.isRegularFile)
    migratedFiles
      .map(
        _.pathAsString.replace(slRoot.pathAsString, "")
      )
      .sorted should contain theSameElementsAs expectedFiles
      .map(_.pathAsString.replace(expectedRoot.pathAsString, ""))
      .toList
      .sorted
  }

  behavior of "Migrate command"
  it should "migrate unversioned on best effort" in {
    withProject("unversioned") { slRoot =>
      withEnvs("SL_ROOT" -> slRoot.pathAsString) {
        assert(new Main().run(Array("migrate")))
        checkMigration(slRoot)
      }
    }
  }
}
