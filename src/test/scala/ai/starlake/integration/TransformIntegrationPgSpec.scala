package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.job.Main

class TransformIntegrationPgSpec extends JDBCIntegrationSpecBase {
  override def templates = starlakeDir / "samples"

  override def localDir = templates / "spark"

  override def sampleDataDir = localDir / "sample-data"

  val jdbcUrl = TestHelper.pgContainer.jdbcUrl
  val jdbcHost = TestHelper.pgContainer.host
  val jdbcPort = TestHelper.pgContainer.mappedPort(5432)
  val envContent =
    s"""
        |env:
        |  myConnectionRef: "postgresql"
        |  loader: "native"
        |  POSTGRES_HOST: "$jdbcHost"
        |  POSTGRES_PORT: "$jdbcPort"
        |  POSTGRES_USER: "test"
        |  POSTGRES_PASSWORD: "test"
        |  POSTGRES_DATABASE: "starlake"
        |""".stripMargin
  val envFile = localDir / "metadata" / "env.PG.sl.yml"
  envFile.write(envContent)
  "Native Postgres Transform" should "succeed" in {
    if (false) {
      withEnvs(
        "SL_ENV"  -> "PG",
        "SL_ROOT" -> localDir.pathAsString
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        Main.run(
          Array("transform", "--name", "sales_kpi.byseller_kpi")
        )
      }
    }
  }
}
