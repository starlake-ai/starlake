package ai.starlake.tests

import ai.starlake.config.Settings
import ai.starlake.job.Main
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.DDLLeaf
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

import java.io.File
import java.nio.file.Files
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.{Failure, Success}

case class StarlakeTest(
  name: String,
  domain: String,
  table: String,
  assertData: StarlakeTestData,
  data: List[StarlakeTestData],
  loadPaths: List[File]
) {

  def getTaskName(): String = name.split('.').last

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
    if (data.nonEmpty) {
      val stmt = conn.createStatement()
      stmt.execute(s"""CREATE SCHEMA IF NOT EXISTS "$domain"""")
      stmt.execute(data)
      stmt.close()
    }
  }
  def unload(conn: java.sql.Connection): Unit = {
    if (data.nonEmpty) {
      val stmt = conn.createStatement()
      stmt.execute(s"""DROP TABLE "$domain"."$table" CASCADE""")
      stmt.close()
    }
  }
}

object StarlakeTestData {
  def createSchema(domainName: String, conn: java.sql.Connection): Unit = {
    execute(conn, s"""CREATE SCHEMA IF NOT EXISTS "$domainName"""")
  }

  def dropSchema(domainName: String, conn: java.sql.Connection): Unit = {
    execute(conn, s"""DROP SCHEMA IF EXISTS "$domainName" CASCADE""")
  }

  def describeTable(connection: Connection, domainAndTable: String): List[String] = {
    val domAndTbl = domainAndTable.split('.')
    val domain = domAndTbl(0)
    val table = domAndTbl(1)

    val (stmt, rs) = executeQuery(connection, s"""DESCRIBE "$domain"."$table"""")
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
                        |(SELECT * FROM "$targetDomain"."$assertTable" EXCEPT SELECT * FROM "$targetDomain"."$targetTable")
                        |TO '$outputPath' (HEADER, DELIMITER ',') """.stripMargin
    execute(conn, diffSql)
    try {
      val allLines = Files.readAllLines(outputPath.toPath)
      allLines.size() <= 1 // We ignore the header
    } catch {
      case _: Exception => // ignore. File does not exists
        true
    }
  }
  def compareResults(
    testFolder: Directory,
    targetDomain: String,
    targetTable: String,
    taskName: String,
    assertTable: String,
    conn: java.sql.Connection,
    duration: Long
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
      val missingSuccess = outputTableDifferences(
        conn,
        targetDomain,
        targetTable,
        assertTable,
        missingPath
      )

      val notExpectedSuccess = outputTableDifferences(
        conn,
        targetDomain,
        assertTable,
        targetTable,
        notExpectedPath
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
      taskName = taskName,
      testName = testFolder.name,
      missingColumns = missingColumns,
      notExpectedColumns = notExpectedColumns,
      missingRecords = missingPath,
      notExpectedRecords = notExpectedPath,
      success = success,
      None,
      duration
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

  def runTransforms(
    dataAndTests: (
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
    ),
    config: StarlakeTestConfig
  )(implicit originalSettings: Settings): List[StarlakeTestResult] = {
    def params(test: StarlakeTest): Array[String] =
      Array("transform", "--test", "--name", test.name) ++ config.toArgs
    val rootFolder = new File(originalSettings.appConfig.root, "test-reports")
    val testsFolder = new Directory(new File(rootFolder, "transform"))
    run(dataAndTests, params, testsFolder)
  }

  def runLoads(
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
    config: StarlakeTestConfig
  )(implicit originalSettings: Settings): List[StarlakeTestResult] = {
    def params(test: StarlakeTest): Array[String] =
      Array("load", "--test", "--domains", test.domain, "--tables", test.table) ++
      Array("--files", test.loadPaths.map(_.toString).mkString(",")) ++
      config.toArgs

    val rootFolder = new File(originalSettings.appConfig.root, "test-reports")
    val testsFolder = new Directory(new File(rootFolder, "load"))
    run(dataAndTests, params, testsFolder)
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
    params: StarlakeTest => Array[String],
    testsFolder: Directory
  )(implicit originalSettings: Settings): List[StarlakeTestResult] = {
    Class.forName("org.duckdb.DuckDBDriver")
    testsFolder.deleteRecursively()
    testsFolder.createDirectory(force = true, failIfExists = false)
    val (rootData, tests) = dataAndTests
    tests.flatMap { case (domainName, dataAndTables) =>
      val domainFolder = new Directory(new File(testsFolder.jfile, domainName))
      val (domainData, tables) = dataAndTables
      tables.flatMap { case (tableName, dataAndTests) =>
        val tableFolder = new Directory(new File(domainFolder.jfile, tableName))
        val (taskData, tests) = dataAndTests
        tests.map { case (testName, test) =>
          val testFolder = new Directory(new File(tableFolder.jfile, testName))
          testFolder.deleteRecursively()
          testFolder.createDirectory(force = true)
          val dbFilename = new File(testFolder.jfile, s"$testName.db").getPath()
          implicit val settings = createDuckDbSettings(originalSettings, dbFilename)
          import settings.storageHandler
          val schemaHandler = new SchemaHandler(storageHandler())(settings)
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
          // also concurrent access is not supported in embedded test mode
          val start = System.currentTimeMillis()
          val result =
            new Main().run(
              params(test),
              schemaHandler
            )(settings) match {
              case Failure(e) =>
                val end = System.currentTimeMillis()
                println(s"Test $domainName.$tableName.$testName failed to run (${e.getMessage})")
                StarlakeTestResult(
                  testFolder.path,
                  domainName,
                  tableName,
                  test.getTaskName(),
                  testName,
                  Nil,
                  Nil,
                  new File(""),
                  new File(""),
                  success = false,
                  exception = Some(e),
                  duration = end - start // in milliseconds
                )
              case Success(_) =>
                val end = System.currentTimeMillis()
                val compareResult = Utils.withResources(
                  DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
                ) { conn =>
                  compareResults(
                    testFolder,
                    test.domain,
                    test.table,
                    test.getTaskName(),
                    "sl_expected",
                    conn,
                    end - start
                  )
                }
                if (compareResult.success) {
                  println(s"Test $domainName.$tableName.$testName succeeded")
                } else {
                  println(s"Test $domainName.$tableName.$testName failed")
                }
                compareResult
            }
          /*
          Utils.withResources(
            DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
          ) { conn =>
            rootData.foreach(_.unload(conn))
            domainData.foreach(_.unload(conn))
            taskData.foreach(_.unload(conn))
          }
          test.unload(dbFilename)
           */
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

  def loadTests(area: Path, testName: Option[String])(implicit
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
    val (domainName, taskName, test) = testName.map { testName =>
      val split = testName.split('.')
      assert(split.length == 3, "Invalid test format. Use 'domainName.taskName.testName'")
      val domainName = split(0)
      val taskName = split(1)
      val test = split(2)
      (domainName, taskName, test)
    } match {
      case None                               => ("", "", "") // empty means load all
      case Some((domainName, taskName, test)) => (domainName, taskName, test)
    }
    val testDir = new File(area.toString)
    val domains = Option(testDir.listFiles)
      .getOrElse(Array())
      .filter(dir => dir.isDirectory && (domainName.isEmpty || domainName == dir.getName))
      .toList
    val rootData = Option(testDir.listFiles)
      .getOrElse(Array())
      .filter(_.isFile)
      .toList
      .flatMap(f => loadDataFile("", f))
    val allTests = domains.map { domainPath =>
      val tasks = domainPath
        .listFiles(taskPath =>
          taskPath.isDirectory && (taskName.isEmpty || taskName == taskPath.getName)
        )
        .toList
      val domainData =
        Option(domainPath.listFiles)
          .getOrElse(Array())
          .filter(_.isFile)
          .toList
          .flatMap(f => loadDataFile("", f))
      val domainTests = tasks.map { taskPath =>
        val tests = taskPath
          .listFiles(testDir =>
            testDir.isDirectory && (testName.isEmpty || test == testDir.getName)
          )
          .toList
        val taskData = Option(taskPath.listFiles)
          .getOrElse(Array())
          .filter(_.isFile)
          .toList
          .flatMap(f => loadDataFile("", f))
        val taskTests = tests.flatMap { testPath =>
          val dataPaths = testPath.listFiles(f => f.isFile && !f.getName.startsWith("_")).toList
          val assertFileCsv = new File(testPath, "_expected.csv")
          val assertFileJson = new File(testPath, "_expected.json")
          val domainName = domainPath.getName
          val taskName = taskPath.getName
          val domain = schemaHandler.domains().find(_.finalName == domainName)
          val table = domain.flatMap { d =>
            d.tables.find(_.finalName == taskName)
          }
          val assertFile =
            if (assertFileCsv.exists()) {
              Some(assertFileCsv)
            } else if (assertFileJson.exists()) {
              Some(assertFileJson)
            } else {
              None
            }
          val expectedCreateTable = (table, assertFile) match {
            case (Some(table), Some(assertFile)) =>
              val fields = table.ddlMapping("duckdb", schemaHandler)
              val cols = fields
                .map { case field: DDLLeaf =>
                  s""""${field.name}" ${field.tpe}"""
                }
                .mkString(", ")
              s"""CREATE TABLE "$domainName"."sl_expected" ($cols);
                 |COPY "$domainName"."sl_expected" FROM '${assertFile.toString}';""".stripMargin
            case (None, Some(assertFile)) =>
              s"CREATE TABLE $domainName.sl_expected AS SELECT * FROM '${assertFile.toString}';"
            case _ => ""
          }

          val assertContent = expectedCreateTable
          val task = schemaHandler.tasks().find(_.name == s"$domainName.$taskName")
          (table, task) match {
            case (Some(table), None) =>
              val (preloadPaths, loadPaths) =
                dataPaths.partition { path =>
                  val name = path.getName
                  val isDataFile =
                    name.equals(s"$domainName.$taskName.json") ||
                    name.equals(s"$domainName.$taskName.csv")
                  isDataFile
                }

              // if only one file with the table name is found. This means that we are loading in overwrite mode.
              // In this case we do not need the preload.
              // The file in that case designates the load path
              val (testDataList, testLoadPaths) =
                if (loadPaths.isEmpty && preloadPaths.size == 1) {
                  (Nil, preloadPaths)
                } else {
                  val pre = preloadPaths.flatMap { dataPath =>
                    loadDataFile(testPath.getName, dataPath)
                  }
                  val matchedPaths = loadPaths.filter { loadPath =>
                    table.pattern.matcher(loadPath.getName).matches()
                  }
                  val unmatchedPaths = loadPaths.toSet -- matchedPaths.toSet
                  if (unmatchedPaths.nonEmpty) {
                    val testName = testPath.getName
                    // scalastyle:off
                    println(
                      s"Load Test $domainName.$taskName.$testName has unmatched load files: ${unmatchedPaths.map(_.getName).mkString(", ")}"
                    )
                  }
                  (pre, matchedPaths)
                }

              val assertData =
                StarlakeTestData(domainName, table.finalName, "_expected", assertContent)
              Some(
                testPath.getName -> StarlakeTest(
                  s"$domainName.$taskName",
                  domainName,
                  table.finalName,
                  assertData,
                  testDataList,
                  testLoadPaths
                )
              )
            case (None, Some(task)) =>
              val testDataList =
                dataPaths.flatMap { dataPath =>
                  loadDataFile(testPath.getName, dataPath)
                }
              val assertData = StarlakeTestData(task.domain, task.table, "_expected", assertContent)
              Some(
                testPath.getName -> StarlakeTest(
                  s"$domainName.$taskName",
                  task.domain,
                  task.table,
                  assertData,
                  testDataList,
                  Nil
                )
              )
            case (Some(_), Some(_)) =>
              // scalastyle:off
              println(
                s"Table / Task $domainName.$taskName found in tasks load and transform. Please rename one of them"
              )
              None
            case (None, None) =>
              // scalastyle:off
              println(s"Table / Task $domainName.$taskName not found")
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
              val result = bufferedSource.getLines().mkString("\n")
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
