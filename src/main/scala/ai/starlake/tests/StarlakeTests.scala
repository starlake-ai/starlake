package ai.starlake.tests

import ai.starlake.config.{DatasetArea, Settings}

import java.io.File
import scala.io.Source

case class StarlakeTest(
  domain: String,
  table: String,
  test: String,
  data: String
) {
  def run(conn: java.sql.Connection)(implicit
    settings: Settings
  ): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(s"CREATE SCHEMA IF NOT EXISTS $domain")
    stmt.execute(data)
    stmt.close()

  }
}
object StarlakeTest {

  def run(
    tests: List[(String, List[(String, List[(String, List[StarlakeTest])])])],
    conn: java.sql.Connection
  )(implicit settings: Settings): Unit = {
    tests.foreach { case (domainName, schemas) =>
      schemas.foreach { case (schemaName, tests) =>
        tests.foreach { case (testName, data) =>
          data.foreach { d =>
            d.run(conn)
          }
        }
      }
    }
  }

  def drop(
    tests: List[(String, List[(String, List[(String, List[StarlakeTest])])])],
    conn: java.sql.Connection
  )(implicit settings: Settings) = {
    val stmt = conn.createStatement()
    tests.foreach { case (domainName, schemas) =>
      schemas.foreach { case (schemaName, tests) =>
        tests.foreach { case (testName, data) =>
          data.foreach { d =>
            stmt.execute(s"DROP SCHEMA IF EXISTS ${d.domain} CASCADE")
          }
        }
      }
    }
    stmt.close()
  }

  def load()(implicit
    settings: Settings
  ): List[(String, List[(String, List[(String, List[StarlakeTest])])])] = {
    val testDir = new File(DatasetArea.tests.toString)
    val domains = testDir.listFiles.filter(_.isDirectory).toList
    val allTests = domains.map { domainPath =>
      val schemas = domainPath.listFiles(_.isDirectory).toList
      val domainTests = schemas.map { schemaPath =>
        val tests = schemaPath.listFiles(_.isDirectory).toList
        val schemaTests = tests.map { testPath =>
          val testData = testPath
            .listFiles(_.isFile)
            .toList
            .flatMap { dataPath =>
              val dataName = dataPath.getName()
              val components = dataName.split('.')
              val filterOK = components.length == 3
              if (filterOK) {
                val domainName = components(0)
                val schemaName = components(1)
                val ext = components(2)
                val extOK = Set("json", "csv", "sql").contains(ext)
                if (extOK) {
                  val dataContent =
                    ext match {
                      case "json" | "csv" =>
                        s"CREATE TABLE $domainName.$schemaName AS SELECT * FROM '${dataPath.getPath}';"
                      case "sql" =>
                        val bufferedSource = Source.fromFile(dataPath.getPath)
                        val result = bufferedSource.getLines.mkString("\n")
                        bufferedSource.close
                        result
                      case _ => ""
                    }

                  Some(StarlakeTest(domainName, schemaName, testPath.getName(), dataContent))
                } else {
                  None
                }
              } else {
                None
              }
            }
          testPath.getName() -> testData
        }
        schemaPath.getName() -> schemaTests
      }
      domainPath.getName() -> domainTests
    }
    allTests
  }

}
