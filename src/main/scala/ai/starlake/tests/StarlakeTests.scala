package ai.starlake.tests

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Main
import ai.starlake.job.transform.TransformTestConfig
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils

import java.io.File
import java.nio.file.Files
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
import scala.reflect.io.Directory
import scala.util.Failure

case class StarlakeTest(
  name: String,
  domain: String,
  table: String,
  assertData: StarlakeTestData,
  data: List[StarlakeTestData]
) {
  def load(conn: java.sql.Connection): Unit = {
    data.foreach { d =>
      d.load(conn)
    }
    assertData.load(conn)
  }

  def unload(dbFilename: String): Unit = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
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
case class StarlakeTestResult(
  testFolder: String,
  domainName: String,
  tableName: String,
  testName: String,
  missingColumns: List[String],
  notExpectedColumns: List[String],
  missingRecords: File,
  notExpectedRecords: File,
  success: Boolean
) {
  def html(): Unit = {
    val missingLines = Files.readAllLines(missingRecords.toPath).asScala.toList.tail
    val notExpectedLines = Files.readAllLines(notExpectedRecords.toPath).asScala.toList.tail
    var builder = new StringBuilder
    builder.append(s"<h2>Test: $domainName.$tableName.$testName</h2>")
    if (missingColumns.nonEmpty) {
      builder.append("<h3>Missing columns</h3>")
      builder.append("<ul>")
      missingColumns.foreach { c =>
        builder.append(s"<li>$c</li>")
      }
      builder.append("</ul>")
    }
    if (notExpectedColumns.nonEmpty) {
      builder.append("<h3>Not expected columns</h3>")
      builder.append("<ul>")
      notExpectedColumns.foreach { c =>
        builder.append(s"<li>$c</li>")
      }
      builder.append("</ul>")
    }
    if (missingLines.nonEmpty) {
      builder.append("<h3>Missing records</h3>")
      builder.append("<table>")
      missingLines.foreach { l =>
        builder.append("<tr>")
        l.split(",").foreach { c =>
          builder.append(s"<td>$c</td>")
        }
        builder.append("</tr>")
      }
      builder.append("</table>")
    }
    if (notExpectedLines.nonEmpty) {
      builder.append("<h3>Not expected records</h3>")
      builder.append("<table>")
      notExpectedLines.foreach { l =>
        builder.append("<tr>")
        l.split(",").foreach { c =>
          builder.append(s"<td>$c</td>")
        }
        builder.append("</tr>")
      }
      builder.append("</table>")
    }
    val html = builder.toString()
    Files.write(new File(testFolder, "index.html").toPath, html.getBytes())

  }

}

object StarlakeTestData {
  def createSchema(domainName: String, conn: java.sql.Connection): Unit = {
    execute(conn, s"CREATE SCHEMA IF NOT EXISTS $domainName")
  }

  def dropSchema(domainName: String, conn: java.sql.Connection): Unit = {
    execute(conn, s"DROP SCHEMA IF EXISTS $domainName CASCADE")
  }

  def describeTable(connection: Connection, table: String): List[String] = {
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

  def outputTableDifferences(
    conn: Connection,
    targetDomain: String,
    targetTable: String,
    assertTable: String,
    outputPath: File
  ): Boolean = {
    val diffSql = s"""COPY
                        |(SELECT * FROM $targetDomain.$assertTable EXCEPT SELECT * FROM $targetDomain.$targetTable)
                        |TO '$outputPath' (HEADER, DELIMITER ',') """.stripMargin
    execute(conn, diffSql)
    try {
      val allLines = Files.readAllLines(outputPath.toPath)
      allLines.size() <= 1
    } catch {
      case _: Exception => // ignore. File does not exists
        true
    }
  }
  def compareResults(
    testFolder: Directory,
    targetDomain: String,
    targetTable: String,
    assertTable: String,
    conn: java.sql.Connection
  ): StarlakeTestResult = {
    val targetColumns = describeTable(conn, s"$targetDomain.$targetTable")
    val assertColumns = describeTable(conn, s"$targetDomain.$assertTable")
    val missingColumns = assertColumns.diff(targetColumns)
    val notExpectedColumns = targetColumns.diff(assertColumns)
    val notExpectedPath = new File(testFolder.jfile, "not_expected.csv")
    val missingPath = new File(testFolder.jfile, "missing.csv")
    var success = true
    if (missingColumns.nonEmpty) {
      println(s"Missing columns: ${missingColumns.mkString(", ")}")
    }
    if (notExpectedColumns.nonEmpty) {
      println(s"Not expected columns: ${notExpectedColumns.mkString(", ")}")
    }

    if (missingColumns.isEmpty && notExpectedColumns.isEmpty) {
      val notExpectedSuccess = outputTableDifferences(
        conn,
        targetDomain,
        targetTable,
        assertTable,
        notExpectedPath
      )

      val missingSuccess = outputTableDifferences(
        conn,
        targetDomain,
        assertTable,
        targetTable,
        missingPath
      )

      success = notExpectedSuccess && missingSuccess
    } else {
      Files.write(
        notExpectedPath.toPath,
        "number of columns does not match. Not expected data could not be computed".getBytes()
      )
      Files.write(
        missingPath.toPath,
        "number of columns does not match. Missing data could not be computed".getBytes()
      )
      success = false
    }

    StarlakeTestResult(
      testFolder.path,
      domainName = targetDomain,
      tableName = targetTable,
      testName = testFolder.name,
      missingColumns = missingColumns,
      notExpectedColumns = notExpectedColumns,
      missingRecords = missingPath,
      notExpectedRecords = notExpectedPath,
      success = success
    )
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
    dataAndTests: (
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
    ),
    config: TransformTestConfig
  ): List[StarlakeTestResult] = {
    val originalSettings: Settings = Settings(Settings.referenceConfig)
    val testsFolder = new Directory(new File(originalSettings.appConfig.root, "test-results"))
    testsFolder.deleteRecursively()
    testsFolder.createDirectory(force = true, failIfExists = false)
    val (rootData, tests) = dataAndTests
    tests.flatMap { case (domainName, dataAndTables) =>
      val (domainData, tables) = dataAndTables
      tables.flatMap { case (tableName, dataAndTests) =>
        val (taskData, tests) = dataAndTests
        tests.map { case (testName, test) =>
          val testFolder = new Directory(new File(testsFolder.jfile, testName))
          testFolder.deleteRecursively()
          testFolder.createDirectory(force = true)
          val dbFilename = new File(testFolder.jfile, s"$testName.db").getPath()
          implicit val settings = createDuckDbSettings(originalSettings, dbFilename)
          import settings.storageHandler
          val schemaHandler = new SchemaHandler(storageHandler())
          Utils.withResources(
            DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
          ) { conn =>
            createSchema(domainName, conn)
            rootData.foreach(_.load(conn))
            domainData.foreach(_.load(conn))
            taskData.foreach(_.load(conn))

            test.load(conn)
          }
          // We close the connection here since the transform will open its own
          // and concurrent access is not supported in embedded test mode
          val params = Array("transform", "--test", "--name", test.name) ++ config.toArgs
          val result =
            new Main().run(
              Array("transform", "--test", "--name", test.name),
              schemaHandler
            ) match {
              case Failure(e) =>
                throw e
              case _ =>
                Utils.withResources(
                  DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
                ) { conn =>
                  compareResults(testFolder, test.domain, test.table, "sl_assert", conn)
                }
            }
          Utils.withResources(
            DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
          ) { conn =>
            rootData.foreach(_.unload(conn))
            domainData.foreach(_.unload(conn))
            taskData.foreach(_.unload(conn))
          }
          test.unload(dbFilename)
          if (result.success) {
            println(s"Test $domainName.$tableName.$testName succeeded")
          } else {
            println(s"Test $domainName.$tableName.$testName failed")
          }
          result
        }
      }
    }
  }

  private def createDuckDbSettings(originalSettings: Settings, dbFilename: String): Settings = {
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
    tests: List[(String, List[(String, List[(String, StarlakeTest)])])],
    conn: java.sql.Connection
  )(implicit settings: Settings) = {
    tests.foreach { case (domainName, tables) =>
      dropSchema(domainName, conn)
      tables.foreach { case (tableName, tests) =>
        tests.foreach { case (testName, test) =>
          test.data.foreach { d =>
            dropSchema(d.domain, conn)
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
