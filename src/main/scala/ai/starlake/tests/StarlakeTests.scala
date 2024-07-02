package ai.starlake.tests

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Main
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.DDLLeaf
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.reflect.io.Directory
import scala.util.{Failure, Success}

case class StarlakeTestCoverage(
  testedDomains: Set[String],
  testedTables: Set[String],
  untestedDomains: List[String],
  untestedTables: List[String]
) {
  def getTestedDomains(): util.List[String] = testedDomains.toList.sorted.asJava
  def getTestedTables(): util.List[String] = testedTables.toList.sorted.asJava
  def getUntestedDomains(): util.List[String] = untestedDomains.sorted.asJava
  def getUntestedTables(): util.List[String] = untestedTables.sorted.asJava

  def getDomainCoveragePercent(): Int = {
    val totalDomains = testedDomains.size + untestedDomains.size
    if (totalDomains == 0) 0 else testedDomains.size * 100 / totalDomains
  }
  def getTableCoveragePercent(): Int = {
    val totalTables = testedTables.size + untestedTables.size
    if (totalTables == 0) 0 else testedTables.size * 100 / totalTables
  }

}
case class StarlakeTest(
  name: String,
  domain: String,
  table: String,
  assertData: Array[StarlakeTestData],
  data: List[StarlakeTestData],
  incomingFiles: List[File]
) {

  def getTaskName(): String = name.split('.').last

  def load(conn: java.sql.Connection): Unit = {
    data.foreach { d =>
      d.load(conn)
    }
    assertData.foreach(_.load(conn))
  }

  def unload(dbFilename: String): Unit = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
    ) { conn =>
      data.foreach { d =>
        d.unload(conn)
      }
      assertData.foreach(_.unload(conn))
    }
  }
}
case class StarlakeTestData(
  domain: String,
  table: String,
  createTableExpression: String,
  expectationAsSql: Option[String],
  expectationName: String
) {
  def load(conn: java.sql.Connection): Unit = {
    if (createTableExpression.nonEmpty) {
      val stmt = conn.createStatement()
      stmt.execute(s"""CREATE SCHEMA IF NOT EXISTS "$domain"""")
      stmt.execute(createTableExpression)
      stmt.close()
    }
  }
  def unload(conn: java.sql.Connection): Unit = {
    if (createTableExpression.nonEmpty) {
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
// Call transpile
  def outputTableDifferences(
    conn: Connection,
    targetDomain: String,
    targetTable: String,
    assertDatum: StarlakeTestData,
    outputPath: File
  ): Boolean = {
    val sql = assertDatum.expectationAsSql.getOrElse("*")
    val assertTable = assertDatum.table
    val selectPattern = "\\?i(^|\\s)SELECT(\\s|$)".r
    val selectStatement =
      selectPattern.findFirstMatchIn(sql) match {
        case Some(_) =>
          sql
        case None =>
          val columnNames = sql
          s"""SELECT $columnNames FROM "$targetDomain"."$assertTable" EXCEPT SELECT $columnNames FROM "$targetDomain"."$targetTable""""
      }

    val diffSql = s"""COPY
                        |($selectStatement)
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
    assertData: Array[StarlakeTestData],
    conn: java.sql.Connection,
    duration: Long
  ): Array[StarlakeTestResult] = {
    assertData.map { assertDatum =>
      val assertTable = assertDatum.table
      val targetColumns = describeTable(conn, s"$targetDomain.$targetTable")
      val assertColumns = describeTable(conn, s"$targetDomain.${assertTable}")
      val missingColumns = assertColumns.diff(targetColumns)

      val notExpectedColumns =
        if (assertDatum.expectationName.isEmpty)
          targetColumns.diff(assertColumns)
        else
          Nil
      val notExpectedPath =
        new File(testFolder.jfile, s"not_expected${assertDatum.expectationName}.csv")
      val missingPath = new File(testFolder.jfile, s"missing${assertDatum.expectationName}.csv")
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
          assertDatum,
          missingPath
        )

        val notExpectedSuccess = outputTableDifferences(
          conn,
          targetDomain,
          assertTable,
          assertDatum,
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
        expectationName = assertDatum.expectationName,
        missingColumns = missingColumns,
        notExpectedColumns = notExpectedColumns,
        missingRecords = missingPath,
        notExpectedRecords = notExpectedPath,
        success = success,
        None,
        duration
      )
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
      ],
      List[(String, String)]
    ),
    config: StarlakeTestConfig
  )(implicit originalSettings: Settings): (List[StarlakeTestResult], StarlakeTestCoverage) = {
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
      ],
      List[(String, String)]
    ),
    config: StarlakeTestConfig
  )(implicit originalSettings: Settings): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    def params(test: StarlakeTest): Array[String] =
      Array("load", "--test", "--domains", test.domain, "--tables", test.table) ++
      Array("--files", test.incomingFiles.map(_.toString).mkString(",")) ++
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
      ],
      List[(String, String)]
    ),
    params: StarlakeTest => Array[String],
    testsFolder: Directory
  )(implicit originalSettings: Settings): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    Class.forName("org.duckdb.DuckDBDriver")
    testsFolder.deleteRecursively()
    testsFolder.createDirectory(force = true, failIfExists = false)
    val (rootData, tests, domainsAndTables) = dataAndTests
    val testResults = {
      tests.flatMap { case (domainName, dataAndTables) =>
        val domainFolder = new Directory(new File(testsFolder.jfile, domainName))
        val (domainData, tables) = dataAndTables
        tables.flatMap { case (tableName, dataAndTests) =>
          val tableFolder = new Directory(new File(domainFolder.jfile, tableName))
          val (taskData, tests) = dataAndTests
          tests.flatMap { case (testName, test) =>
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
                  Array(
                    StarlakeTestResult(
                      testFolder.path,
                      domainName,
                      tableName,
                      test.getTaskName(),
                      testName,
                      expectationName = "",
                      Nil,
                      Nil,
                      new File(""),
                      new File(""),
                      success = false,
                      exception = Some(e),
                      duration = end - start // in milliseconds
                    )
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
                      test.assertData,
                      conn,
                      end - start
                    )
                  }
                  if (compareResult.forall(_.success)) {
                    println(s"Test $domainName.$tableName.$testName succeeded")
                  } else {
                    println(s"Test $domainName.$tableName.$testName failed")
                  }
                  compareResult
              }
            result
          }
        }
      }
    }
    val testedDomains = testResults.map(result => result.domainName).toSet
    val testedTables = testResults.map(result => result.domainName + "." + result.tableName).toSet
    val untestedDomains = domainsAndTables
      .filter { case (domain, _) =>
        !testedDomains.contains(domain)
      }
      .map(_._1)
    val untestedTables = domainsAndTables
      .map { case (domain, table) => s"$domain.$table" }
      .filter { table =>
        !testedTables.contains(table)
      }
    val coverage = StarlakeTestCoverage(
      testedDomains,
      testedTables,
      untestedDomains,
      untestedTables
    )
    (testResults, coverage)
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

  /** @param area
    *   load tests or transform tests folder
    * @param testName
    *   Are we loading a single tests or all tests
    * @param settings
    *   context
    */
  type DomainName = String
  type TableOrTaskName = String
  def loadTests(area: Path, testName: Option[String])(implicit
    settings: Settings
  ): (
    List[StarlakeTestData], // Root Test Data
    List[
      (
        DomainName, // Domain Name
        (
          List[StarlakeTestData], // Domain Test Data
          List[
            (
              TableOrTaskName, // Table or Task Name
              (
                List[StarlakeTestData], // Table or Task Test Data
                List[(String, StarlakeTest)] // Test Name, Test
              )
            )
          ]
        )
      )
    ],
    List[(String, String)]
  ) = {
    import settings.storageHandler
    val schemaHandler = new SchemaHandler(storageHandler())

    // Load single test
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

    // Domain names we are willing to test
    val domains = Option(testDir.listFiles)
      .getOrElse(Array())
      .filter(dir => dir.isDirectory && (domainName.isEmpty || domainName == dir.getName))
      .toList

    // load all test files as StarlakeTestData objects
    val rootTestData: List[StarlakeTestData] = Option(testDir.listFiles)
      .getOrElse(Array())
      .filter(_.isFile)
      .toList
      .flatMap(f => testDataFromFile("", "", f))

    val allTests = domains.map { domainPath =>
      val taskOrTableFolders = domainPath
        .listFiles(taskPath =>
          taskPath.isDirectory && (taskName.isEmpty || taskName == taskPath.getName)
        )
        .toList

      val domainTestData =
        Option(domainPath.listFiles)
          .getOrElse(Array())
          .filter(_.isFile)
          .toList
          .flatMap(f => testDataFromFile("", "", f))

      val domainTests = taskOrTableFolders.map { taskOrTableFolder =>
        val testFolders = taskOrTableFolder
          .listFiles(testDir =>
            testDir.isDirectory && (testName.isEmpty || test == testDir.getName)
          )
          .toList

        // test data for all tests of a single table
        val taskOrTableTestData: List[StarlakeTestData] = Option(taskOrTableFolder.listFiles)
          .getOrElse(Array())
          .filter(_.isFile)
          .toList
          .flatMap(f => testDataFromFile("", "", f))

        val taskOrTableTests: List[(String, StarlakeTest)] = testFolders.flatMap { testFolder =>
          // Al files that do not start with an 'sl_' are considered data files
          // files that end with ".sql" will soon be considered as tests to run against
          // the output and compared against the expected output in the "_expected_filename" file
          val testDataFiles = testFolder
            .listFiles(f => f.isFile && !f.getName.startsWith("_") && !f.getName.endsWith(".sql"))
            .toList

          // assert files start with an '_expected' prefix (for now we supports only one assert file)
          val assertFileCsv = new File(testFolder, "_expected.csv")
          val assertFileJson = new File(testFolder, "_expected.json")
          val assertDataFile =
            if (assertFileCsv.exists()) {
              Some(assertFileCsv)
            } else if (assertFileJson.exists()) {
              Some(assertFileJson)
            } else {
              None
            }
          val domainName = domainPath.getName
          val taskOrTableFolderName = taskOrTableFolder.getName

          val assertData = testFolder.listFiles().flatMap { f =>
            val filename = f.getName
            val isDataFile =
              f.isFile &&
              filename.startsWith("_expected") &&
              (filename.endsWith(".json") || f.getName.endsWith(".csv"))
            if (isDataFile) {
              val expectationName =
                filename.substring("_expected".length, filename.lastIndexOf('.'))
              // SELECT * FROM "$targetDomain"."$assertTable" EXCEPT SELECT * FROM "$targetDomain"."$targetTable"
              val expectedCreateTable: String =
                loadDataAsCreateTableExpression(
                  schemaHandler,
                  domainName,
                  "sl_expected" + expectationName,
                  f
                )
              if (expectationName.isEmpty) {
                Some(
                  StarlakeTestData(
                    domainName,
                    "sl_expected",
                    expectedCreateTable,
                    Some("*"),
                    expectationName
                  )
                )
              } else {
                val expectationFile = new File(testFolder, s"$expectationName.sql")
                if (expectationFile.exists()) {
                  val source = Source.fromFile(expectationFile)
                  val sql = source.mkString
                  source.close()
                  Some(
                    StarlakeTestData(
                      domainName,
                      "sl_expected" + expectationName,
                      expectedCreateTable,
                      Some(sql),
                      expectationName
                    )
                  )
                } else {
                  None
                }
              }
            } else {
              None
            }
          }

          val domain = schemaHandler.domains().find(_.finalName == domainName)

          val table = domain.flatMap { d =>
            d.tables.find(_.finalName == taskOrTableFolderName)
          }

          val task = schemaHandler.tasks().find(_.name == s"$domainName.$taskOrTableFolderName")
          (table, task) match {
            case (Some(table), None) =>
              // handle load
              val (preloadFiles, pendingLoadFiles) =
                testDataFiles.partition { path =>
                  val name = path.getName
                  name.equals(s"$domainName.$taskOrTableFolderName.json") ||
                  name.equals(s"$domainName.$taskOrTableFolderName.csv")
                }

              // if only one file with the table name is found. This means that we are loading in overwrite mode.
              // In this case we do not need the preload.
              // The file in that case designates the load path
              val (preloadTestData, incomingFiles) =
                if (pendingLoadFiles.isEmpty && preloadFiles.size == 1) {
                  (Nil, preloadFiles)
                } else {
                  val preloadTestData = preloadFiles.flatMap { dataPath =>
                    testDataFromFile(testFolder.getName, "", dataPath)
                  }
                  // For extra files, we try to match them with the table name to detect invalid files
                  val matchingPatternFiles = pendingLoadFiles.filter { loadPath =>
                    table.pattern.matcher(loadPath.getName).matches()
                  }
                  val unmatchingPatternFiles = pendingLoadFiles.toSet -- matchingPatternFiles.toSet
                  if (unmatchingPatternFiles.nonEmpty) {
                    val testName = testFolder.getName
                    // scalastyle:off
                    println(
                      s"Load Test $domainName.$taskOrTableFolderName.$testName has unmatched load files: ${unmatchingPatternFiles.map(_.getName).mkString(", ")}"
                    )
                  }
                  (preloadTestData, matchingPatternFiles)
                }

              Some(
                testFolder.getName -> StarlakeTest(
                  s"$domainName.$taskOrTableFolderName",
                  domainName,
                  taskOrTableFolderName,
                  assertData,
                  preloadTestData,
                  incomingFiles
                )
              )
            case (None, Some(task)) =>
              // handle transform
              val testDataList =
                testDataFiles.flatMap { dataPath =>
                  testDataFromFile(testFolder.getName, "", dataPath)
                }
              Some(
                testFolder.getName -> StarlakeTest(
                  s"$domainName.$taskOrTableFolderName",
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
                s"Table / Task $domainName.$taskOrTableFolderName found in tasks load and transform. Please rename one of them"
              )
              None
            case (None, None) =>
              // scalastyle:off
              println(s"Table / Task $domainName.$taskOrTableFolderName not found")
              None
          }
        }
        taskOrTableFolder.getName() -> (taskOrTableTestData, taskOrTableTests)
      }
      domainPath.getName() -> (domainTestData, domainTests)
    }
    val domainAndTables =
      if (area.toString == DatasetArea.loadTests.toString)
        schemaHandler.domains().flatMap { dom =>
          dom.tables.map { table =>
            (dom.finalName, table.finalName)
          }
        }
      else
        schemaHandler.tasks().map { task =>
          (task.domain, task.name)
        }
    (rootTestData, allTests, domainAndTables)
  }

  private def loadDataAsCreateTableExpression(
    schemaHandler: SchemaHandler,
    domainName: String,
    tableName: String,
    dataFile: File
  ): String = {

    val domain = schemaHandler.domains().find(_.finalName == domainName)
    val table = domain.flatMap { d =>
      d.tables.find(_.finalName == tableName)
    }
    val expectedCreateTable = table match {
      case (Some(table)) =>
        // We have the table YML file, we thus create the table schema using the YML file ddl mapping feature
        val fields = table.ddlMapping("duckdb", schemaHandler)
        val cols = fields
          .map { case field: DDLLeaf =>
            s""""${field.name}" ${field.tpe}"""
          }
          .mkString(", ")
        val firstLine =
          if (dataFile.getName.endsWith("json"))
            Files
              .readAllLines(Paths.get(dataFile.toString), StandardCharsets.UTF_8)
              .get(0)
              .trim
          else
            ""
        val extraArgs =
          if (firstLine.startsWith("[")) "(FORMAT JSON, ARRAY true)"
          else if (dataFile.getName.endsWith("csv")) "(FORMAT CSV, nullstr 'null')"
          else ""
        s"""CREATE TABLE "$domainName"."$tableName" ($cols);
                 |COPY "$domainName"."$tableName" FROM '${dataFile.toString}' $extraArgs;""".stripMargin
      case None =>
        // Table not present in starlake schema, we let duckdb infer the schema
        val source =
          if (dataFile.getName.endsWith("csv"))
            s"read_csv('${dataFile.toString}', nullstr = 'null')"
          else s"'${dataFile.toString}'"
        s"CREATE TABLE $domainName.$tableName AS SELECT * FROM $source;"
    }
    expectedCreateTable
  }

  /** Preload duckdb with the data contained in the test folder. The data is expected to be in the
    * form of json or csv files and the filename should be the same as the domain & table name we
    * want to preload
    * @param testName:
    *   test folder name
    * @param dataPath:
    *   path to the csv/json file containing the data
    * @param settings
    *   context
    * @return
    */
  private def testDataFromFile(testName: String, expectationName: String, dataPath: File)(implicit
    settings: Settings
  ): Option[StarlakeTestData] = {
    val dataName = dataPath.getName()
    val components = dataName.split('.')
    val filterOK = components.length == 3
    if (filterOK) {
      val testDataDomainName = components(0)
      val testDataTableName = components(1)
      val ext = components(2)
      val extOK = Set("json", "csv").contains(ext)
      if (extOK) {
        val dataAsCreateTableExpression =
          ext match {
            case "json" | "csv" =>
              val schemaHandler = new SchemaHandler(settings.storageHandler())
              loadDataAsCreateTableExpression(
                schemaHandler,
                testDataDomainName,
                testDataTableName,
                dataPath
              )
            case _ => ""
          }

        Some(
          StarlakeTestData(
            testDataDomainName, // Schema name in DuckDB
            testDataTableName, // Table name in DuckDB
            dataAsCreateTableExpression, // csv/json content
            None,
            expectationName
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
