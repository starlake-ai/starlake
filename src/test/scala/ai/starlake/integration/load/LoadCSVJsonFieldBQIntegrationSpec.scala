package ai.starlake.integration.load

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main
import better.files.File

class LoadCSVJsonFieldBQIntegrationSpec extends BigQueryIntegrationSpecBase {
  override def sampleDataDir: File = theSampleFolder / "sample-csv-json"
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT"                   -> theSampleFolder.pathAsString,
        "SL_ENV"                    -> "BQ",
        "SL_INTERMEDIATE_BQ_FORMAT" -> "avro"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("import")
          )
        )
        assert(new Main().run(Array("load")))
      }
    }
  }
}
