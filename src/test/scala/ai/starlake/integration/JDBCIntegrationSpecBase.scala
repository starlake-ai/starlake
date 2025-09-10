package ai.starlake.integration

import ai.starlake.PgContainerHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import better.files.File

class JDBCIntegrationSpecBase extends IntegrationTestBase with PgContainerHelper {

  /** JDBC only database do not support deep json file formats
    * @param dir
    */
  override protected def copyFilesToIncomingDir(dir: File): Unit = {
    super.copyFilesToIncomingDir(dir)
  }

  /** We delete the table before running the test to ensure that the test is run in a clean
    * environment. We do not delete the table after afterwards because we want to be able to inspect
    * the table after the test.
    */
  override protected def cleanup(): Unit = {
    super.cleanup()
    if (sys.env.getOrElse("SL_JDBC_TEST", "false").toBoolean) {
      implicit val settings: Settings = Settings(Settings.referenceConfig, None, None, None)
      val connectionRef = settings.appConfig.connectionRef
      val connection = settings.appConfig.connections(connectionRef)
      val jdbcOptions =
        JdbcDbUtils.jdbcOptions(
          connection.options,
          connection.sparkDatasource().getOrElse("jdbc"),
          None
        )
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), jdbcOptions) { conn =>
        // drop table using jdbc statement connection conn in the lines below
        val allTables = List("sales.customers", "sales.orders", "hr.locations", "hr.sellers")
        allTables.foreach { table =>
          conn
            .createStatement()
            .executeUpdate(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }
}
