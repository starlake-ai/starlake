package ai.starlake.tests

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Main
import ai.starlake.schema.handlers.SchemaHandler

import java.io.File
import java.sql.{Connection, DriverManager}
import scala.io.Source
import scala.util.Failure

case class StarlakeTest(
  domain: String,
  table: String,
  assertData: StarlakeTestData,
  data: List[StarlakeTestData]
) {
  def load(conn: Connection): Unit = {
    data.foreach { d =>
      d.load(conn)
    }
    assertData.load(conn)
  }

  def unload(conn: Connection): Unit = {
    data.foreach { d =>
      d.unload(conn)
    }
    assertData.unload(conn)
  }

}
case class StarlakeTestData(
  domain: String,
  table: String,
  test: String,
  data: String
) {
  def load(conn: java.sql.Connection): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(s"CREATE SCHEMA IF NOT EXISTS $domain")
    stmt.execute(data)
    stmt.close()

  }
  def unload(conn: java.sql.Connection): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(s"DROP TABLE $domain.$table")
    stmt.close()

  }
}

object StarlakeTestData {
  val dbFilename = "/Users/hayssams/tmp/duckdb.db"

  def run(
    tests: List[(String, List[(String, List[(String, StarlakeTest)])])]
  ): Unit = {
    val conn = DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
    tests.foreach { case (domainName, tables) =>
      val stmt = conn.createStatement()
      stmt.execute(s"CREATE SCHEMA IF NOT EXISTS $domainName")
      stmt.close()
      tables.foreach { case (tableName, tests) =>
        tests.foreach { case (testName, test) =>
          test.load(conn)
          implicit val settings = createDuckDbSettings()
          import settings.storageHandler
          val schemaHandler = new SchemaHandler(storageHandler())
          val result =
            try {
              new Main().run(
                Array("transform", "--test", "--name", s"$domainName.$tableName"),
                schemaHandler
              )
            } catch {
              case e: Throwable =>
                Failure(e)
            }
          test.unload(conn)
        }
      }
      val delStmt = conn.createStatement()
      delStmt.execute(s"DROP SCHEMA $domainName CASCADE")
      delStmt.close()
    }
    conn.close()
  }

  private def createDuckDbSettings(): Settings = {
    val originalSettings: Settings = Settings(Settings.referenceConfig)
    originalSettings.copy(appConfig =
      originalSettings.appConfig.copy(
        connections = originalSettings.appConfig.connections.map { case (k, v) =>
          k -> v.copy(
            `type` = "duckdb",
            quote = Some("\""),
            separator = Some("."),
            sparkFormat = None,
            options = Map("url" -> s"jdbc:duckdb:$dbFilename")
          )
        }
      )
    )
  }

  def drop(
    tests: List[(String, List[(String, List[(String, StarlakeTest)])])],
    conn: java.sql.Connection
  )(implicit settings: Settings) = {
    val stmt = conn.createStatement()
    tests.foreach { case (domainName, tables) =>
      stmt.execute(s"DROP SCHEMA IF EXISTS $domainName CASCADE")
      tables.foreach { case (tableName, tests) =>
        tests.foreach { case (testName, test) =>
          test.data.foreach { d =>
            stmt.execute(s"DROP SCHEMA IF EXISTS ${d.domain} CASCADE")
          }
        }
      }
    }
    stmt.close()
  }

  def loadTests()(implicit
    settings: Settings
  ): List[(String, List[(String, List[(String, StarlakeTest)])])] = {
    val testDir = new File(DatasetArea.tests.toString)
    val domains = testDir.listFiles.filter(_.isDirectory).toList
    val allTests = domains.map { domainPath =>
      val tables = domainPath.listFiles(_.isDirectory).toList
      val domainTests = tables.map { tablePath =>
        val tests = tablePath.listFiles(_.isDirectory).toList
        val tableTests = tests.map { testPath =>
          val dataPaths = testPath.listFiles(f => f.isFile && !f.getName().startsWith("_")).toList
          val assertFileCsv = new File(testPath, "_assert.csv")
          val assertFileJson = new File(testPath, "_assert.json")
          val assertFileSql = new File(testPath, "_assert.sql")
          val domainName = domainPath.getName()
          val tableName = tablePath.getName()
          val assertContent =
            if (assertFileCsv.exists()) {
              s"CREATE TABLE $domainName.$tableName AS SELECT * FROM '${assertFileCsv.toString}';"
            } else if (assertFileJson.exists()) {
              s"CREATE TABLE $domainName.$tableName AS SELECT * FROM '${assertFileJson.toString}';"
            } else if (assertFileSql.exists()) {
              val bufferedSource = Source.fromFile(assertFileSql)
              val sql = bufferedSource.getLines.mkString("\n")
              bufferedSource.close
              sql
            } else {
              ""
            }
          val testDataList =
            dataPaths.flatMap { dataPath =>
              val dataName = dataPath.getName()
              val components = dataName.split('.')
              val filterOK = components.length == 3
              if (filterOK) {
                val testDataDomainName = components(0)
                val testDataTableName = components(1)
                val ext = components(2)
                val extOK = Set("json", "csv", "sql").contains(ext)
                if (extOK) {
                  val dataContent =
                    ext match {
                      case "json" | "csv" =>
                        s"CREATE TABLE $testDataDomainName.$testDataTableName AS SELECT * FROM '${dataPath.getPath}';"
                      case "sql" =>
                        val bufferedSource = Source.fromFile(dataPath.getPath)
                        val result = bufferedSource.getLines.mkString("\n")
                        bufferedSource.close
                        result
                      case _ => ""
                    }

                  Some(
                    StarlakeTestData(
                      testDataDomainName,
                      testDataTableName,
                      testPath.getName(),
                      dataContent
                    )
                  )
                } else {
                  None
                }
              } else {
                None
              }
            }
          val assertData = StarlakeTestData(domainName, tableName, "_assert", assertContent)
          testPath.getName() -> StarlakeTest(domainName, tableName, assertData, testDataList)
        }
        tablePath.getName() -> tableTests
      }
      domainPath.getName() -> domainTests
    }
    allTests
  }

}
