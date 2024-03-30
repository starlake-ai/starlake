package ai.starlake.tests

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Main
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils

import java.io.File
import java.nio.file.Files
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Failure

case class StarlakeTest(
  name: String,
  domain: String,
  table: String,
  assertData: StarlakeTestData,
  data: List[StarlakeTestData]
) {
  def load(): Unit = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:${StarlakeTestData.dbFilename}")
    ) { conn =>
      data.foreach { d =>
        d.load(conn)
      }
      assertData.load(conn)
    }
  }

  def unload(): Unit = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:${StarlakeTestData.dbFilename}")
    ) { conn =>
      data.foreach { d =>
        d.unload(conn)
      }
      assertData.unload(conn)
    }
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
    stmt.execute(s"DROP TABLE $domain.$table CASCADE")
    stmt.close()

  }
}

object StarlakeTestData {
  val dbFilename: String = Files.createTempFile("sl_duck", ".db").toString

  def createSchema(domainName: String): Unit = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
    ) { conn =>
      execute(conn, s"CREATE SCHEMA IF NOT EXISTS $domainName")
    }
  }

  def dropSchema(domainName: String): Unit = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
    ) { conn =>
      execute(conn, s"DROP SCHEMA IF EXISTS $domainName CASCADE")
    }
  }

  def decribeTable(connection: Connection, table: String): List[String] = {
    val (stmt, rs) = executeQuery(connection, s"DESCRIBE $table")
    val columns = new ListBuffer[String]()
    while (rs.next()) {
      val name = rs.getString("column_name")
      val type_ = rs.getString("column_type")
      columns.append(name.toLowerCase())
    }
    rs.close()
    stmt.close()

    columns.toList
  }

  def compareResults(targetDomain: String, targetTable: String, assertTable: String) = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
    ) { conn =>
      val targetColumns = decribeTable(conn, s"$targetDomain.$targetTable")
      val assertColumns = decribeTable(conn, s"$targetDomain.$assertTable")
      val missingColumns = assertColumns.diff(targetColumns)
      val notExpectedColumns = targetColumns.diff(assertColumns)

      if (missingColumns.nonEmpty) {
        println(s"Missing columns: ${missingColumns.mkString(", ")}")
      }

      if (notExpectedColumns.nonEmpty) {
        println(s"Not expected columns: ${notExpectedColumns.mkString(", ")}")
      }

      if (missingColumns.isEmpty && notExpectedColumns.isEmpty) {
        val notExpectedSql = s"""COPY
         |(SELECT * FROM $targetDomain.$targetTable EXCEPT SELECT * FROM $targetDomain.$assertTable)
         |TO '/tmp/not_expected.csv' (HEADER, DELIMITER ',') """.stripMargin
        execute(conn, notExpectedSql)

        val missingSql = s"""COPY
         |(SELECT * FROM $targetDomain.$assertTable EXCEPT SELECT * FROM $targetDomain.$targetTable)
         |TO '/tmp/missing.csv' (HEADER, DELIMITER ',') """.stripMargin
        execute(conn, missingSql)
      }
    }
  }

  private def execute(conn: Connection, sql: String): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
  }

  private def executeQuery(conn: Connection, sql: String): (Statement, ResultSet) = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    (stmt, rs)
  }

  def run(
    dataAnddTests: (
      List[StarlakeTestData], // root data
      List[
        (
          String, // domain name
          (
            List[StarlakeTestData], // domain data
            List[(String, (List[StarlakeTestData], List[(String, StarlakeTest)]))] // domaintests
          )
        )
      ]
    )
  ): Unit = {
    new File(dbFilename).delete()
    val (rootData, tests) = dataAnddTests
    tests.foreach { case (domainName, dataAndtables) =>
      createSchema(domainName)
      val (domainData, tables) = dataAndtables
      tables.foreach { case (tableName, dataAndTests) =>
        val (taskData, tests) = dataAndTests
        tests.foreach { case (testName, test) =>
          Utils.withResources(
            DriverManager.getConnection(s"jdbc:duckdb:${StarlakeTestData.dbFilename}")
          ) { conn =>
            rootData.foreach(_.load(conn))
            domainData.foreach(_.load(conn))
            taskData.foreach(_.load(conn))
          }

          test.load()
          implicit val settings = createDuckDbSettings()
          import settings.storageHandler
          val schemaHandler = new SchemaHandler(storageHandler())
          val result =
            new Main().run(
              Array("transform", "--test", "--name", test.name),
              schemaHandler
            ) match {
              case Failure(e) =>
                e.printStackTrace()
                false
              case _ =>
                compareResults(test.domain, test.table, "sl_assert")

            }
          Utils.withResources(
            DriverManager.getConnection(s"jdbc:duckdb:${StarlakeTestData.dbFilename}")
          ) { conn =>
            rootData.foreach(_.unload(conn))
            domainData.foreach(_.unload(conn))
            taskData.foreach(_.unload(conn))
          }
          test.unload()
        }
      }
    }
  }

  private def createDuckDbSettings(): Settings = {
    val originalSettings: Settings = Settings(Settings.referenceConfig)
    originalSettings.copy(appConfig =
      originalSettings.appConfig.copy(
        connections = originalSettings.appConfig.connections.map { case (k, v) =>
          k -> v.copy(
            `type` = "jdbc",
            quote = Some("\""),
            separator = Some("."),
            sparkFormat = None,
            options = Map(
              "url"    -> s"jdbc:duckdb:$dbFilename",
              "driver" -> "org.duckdb.DuckDBDriver"
            )
          )
        }
      )
    )
  }

  def drop(
    tests: List[(String, List[(String, List[(String, StarlakeTest)])])]
  )(implicit settings: Settings) = {
    tests.foreach { case (domainName, tables) =>
      dropSchema(domainName)
      tables.foreach { case (tableName, tests) =>
        tests.foreach { case (testName, test) =>
          test.data.foreach { d =>
            dropSchema(d.domain)
          }
        }
      }
    }
  }

  def loadTests()(implicit
    settings: Settings
  ): (
    List[StarlakeTestData],
    List[
      (
        String,
        (
          List[StarlakeTestData],
          List[(String, (List[StarlakeTestData], List[(String, StarlakeTest)]))]
        )
      )
    ]
  ) = {
    import settings.storageHandler
    val schemaHandler = new SchemaHandler(storageHandler())

    val testDir = new File(DatasetArea.tests.toString)
    val domains = testDir.listFiles.filter(_.isDirectory).toList
    val rootData = testDir.listFiles.filter(_.isFile).toList.flatMap(f => loadDataFile("", f))
    val allTests = domains.map { domainPath =>
      val tasks = domainPath.listFiles(_.isDirectory).toList
      val domainData =
        domainPath.listFiles.filter(_.isFile).toList.flatMap(f => loadDataFile("", f))
      val domainTests = tasks.map { taskPath =>
        val tests = taskPath.listFiles(_.isDirectory).toList
        val taskData = taskPath.listFiles.filter(_.isFile).toList.flatMap(f => loadDataFile("", f))
        val taskTests = tests.flatMap { testPath =>
          val dataPaths = testPath.listFiles(f => f.isFile && !f.getName().startsWith("_")).toList
          val assertFileCsv = new File(testPath, "_assert.csv")
          val assertFileJson = new File(testPath, "_assert.json")
          val assertFileSql = new File(testPath, "_assert.sql")
          val domainName = domainPath.getName()
          val taskName = taskPath.getName()
          val assertContent =
            if (assertFileCsv.exists()) {
              s"CREATE TABLE $domainName.sl_assert AS SELECT * FROM '${assertFileCsv.toString}';"
            } else if (assertFileJson.exists()) {
              s"CREATE TABLE $domainName.sl_assert AS SELECT * FROM '${assertFileJson.toString}';"
            } else if (assertFileSql.exists()) {
              val bufferedSource = Source.fromFile(assertFileSql)
              val sql = bufferedSource.getLines.mkString("\n")
              bufferedSource.close
              s"CREATE TABLE $domainName.sl_assert AS SELECT * $sql"
            } else {
              ""
            }
          val testDataList =
            dataPaths.flatMap { dataPath =>
              loadDataFile(testPath.getName(), dataPath)
            }
          val task = schemaHandler.tasks().find(_.name == s"$domainName.$taskName")
          task match {
            case Some(t) =>
              val assertData = StarlakeTestData(t.domain, t.table, "_assert", assertContent)
              Some(
                testPath.getName() -> StarlakeTest(
                  s"$domainName.$taskName",
                  t.domain,
                  t.table,
                  assertData,
                  testDataList
                )
              )
            case None =>
              // scalastyle:off
              println(s"Task $domainName.$taskName not found")
              None
          }
        }
        taskPath.getName() -> (taskData, taskTests)
      }
      domainPath.getName() -> (domainData, domainTests)
    }
    (rootData, allTests)
  }

  private def loadDataFile(testName: String, dataPath: File) = {
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
            testName,
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
}
