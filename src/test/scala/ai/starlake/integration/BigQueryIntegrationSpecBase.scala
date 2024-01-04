package ai.starlake.integration

class BigQueryIntegrationSpecBase extends IntegrationTestBase {

  val directoriesToClear = List("incoming", "audit", "datasets", "diagrams")

  protected def clearDataDirectories(copy: Boolean = true): Unit = {
    directoriesToClear.foreach { dir =>
      val path = localDir / dir
      if (path.exists) {
        path.delete()
      }
    }
    if (copy)
      sampleDataDir.copyTo(incomingDir)

  }

  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {}
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      clearDataDirectories(false)
    }
  }
}
