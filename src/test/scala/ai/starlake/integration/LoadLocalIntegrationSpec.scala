package ai.starlake.integration

import ai.starlake.job.Main

class LoadLocalIntegrationSpec extends JDBCIntegrationSpecBase {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override val incomingDir = localDir / "incoming"
  override def sampleDataDir = localDir / "sample-data"
  "Import / Load / Transform Local" should "succeed" in {
    withEnvs(
      "SL_ROOT"                                       -> localDir.pathAsString,
      "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
      "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
      "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)
      val hrDir = incomingDir / "hr"
      hrDir.list(_.isRegularFile).foreach(_.delete())
      val salesDir = incomingDir / "sales"
      salesDir.list(_.isRegularFile).foreach(_.delete())
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
    }
  }

}
