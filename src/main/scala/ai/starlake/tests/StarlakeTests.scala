package ai.starlake.tests

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Main
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.ConnectionType.JDBC
import ai.starlake.schema.model.{DDLLeaf, EnvDesc}
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
import scala.util.{Failure, Success, Try}

case class StarlakeTestCoverage(
  testedDomains: Set[String],
  testedTables: Set[String],
  untestedDomains: List[String],
  untestedTables: List[String]
) {
  def merge(other: StarlakeTestCoverage): StarlakeTestCoverage = {
    StarlakeTestCoverage(
      testedDomains ++ other.testedDomains,
      testedTables ++ other.testedTables,
      untestedDomains ++ other.untestedDomains,
      untestedTables ++ other.untestedTables
    )
  }
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
  expectations: Array[StarlakeTestData],
  data: List[StarlakeTestData],
  incomingFiles: List[File]
) {

  def getTaskName(): String = name.split('.').last

  def load(conn: java.sql.Connection): Unit = {
    data.foreach { d =>
      d.load(conn)
    }
    expectations.foreach(_.load(conn))
  }

  def unload(dbFilename: String): Unit = {
    Utils.withResources(
      DriverManager.getConnection(s"jdbc:duckdb:$dbFilename")
    ) { conn =>
      data.foreach { d =>
        d.unload(conn)
      }
      expectations.foreach(_.unload(conn))
    }
  }
}
case class StarlakeTestData(
  domain: String,
  table: String,
  createTableExpression: String,
  expectationAsSql: Option[String],
  expectationName: String,
  filename: String,
  incoming: Boolean = false
) {
  def load(conn: java.sql.Connection): Unit = {
    if (!incoming && createTableExpression.nonEmpty) {
      val stmt = conn.createStatement()
      stmt.execute(s"""CREATE SCHEMA IF NOT EXISTS "$domain"""")
      println(createTableExpression)
      stmt.execute(createTableExpression)
      stmt.close()
    }
  }
  def unload(conn: java.sql.Connection): Unit = {
    if (!incoming && createTableExpression.nonEmpty) {
      val stmt = conn.createStatement()
      stmt.execute(s"""DROP TABLE "$domain"."$table" CASCADE""")
      stmt.close()
    }
  }
}

object StarlakeTestData {
  def createDuckDbSettings(originalSettings: Settings, dbFilename: String): Settings = {
    originalSettings.copy(appConfig =
      originalSettings.appConfig.copy(
        connections = originalSettings.appConfig.connections.map { case (k, v) =>
          k -> v.copy(
            `type` = JDBC,
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

  def getFile(
    load: Boolean,
    domainName: Option[String],
    tableName: Option[String],
    testName: Option[String],
    filename: String,
    incoming: Boolean
  )(implicit settings: Settings): File = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    (domainName, tableName, testName) match {
      case (Some(domain), Some(table), Some(test)) =>
        val prefix = if (incoming) "_incoming." else ""
        new File(path.toString, s"$domain/$table/$test/$prefix$filename")
      case (Some(domain), Some(table), None) =>
        new File(path.toString, s"$domain/$table/$filename")
      case (Some(domain), None, None) =>
        new File(path.toString, s"$domain/$filename")
      case (None, None, None) =>
        new File(path.toString, filename)
      case _ =>
        throw new IllegalArgumentException("Invalid arguments")
    }
  }

  def createSchema(domainName: String, conn: java.sql.Connection): Unit = {
    execute(conn, s"""CREATE SCHEMA IF NOT EXISTS "$domainName"""")
  }

  def dropSchema(domainName: String, conn: java.sql.Connection): Unit = {
    execute(conn, s"""DROP SCHEMA IF EXISTS "$domainName" CASCADE""")
  }

  def describe(connection: Connection, domainAndTableOrQuery: String): List[String] = {
    val selectPattern = "(?i)SELECT\\s".r
    val statement =
      selectPattern.findFirstMatchIn(domainAndTableOrQuery) match {
        case Some(_) =>
          domainAndTableOrQuery
        case None =>
          val domAndTbl = domainAndTableOrQuery.split('.')
          val domain = domAndTbl(0)
          val table = domAndTbl(1)
          s""""$domain"."$table""""
      }
    val (stmt, rs) = executeQuery(connection, s"""DESCRIBE $statement""")
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

  private def expectationQuery(
    targetDomain: String,
    targetTable: String,
    assertDatum: StarlakeTestData,
    missing: Boolean
  ): String = {
    val sql = assertDatum.expectationAsSql.getOrElse("*")
    val assertTable = assertDatum.table
    val selectPattern = "(?i)SELECT\\s".r
    val selectStatement =
      selectPattern.findFirstMatchIn(sql) match {
        case Some(_) =>
          if (missing)
            s"""$sql EXCEPT SELECT * FROM "$targetDomain"."$assertTable""""
          else // not expected
            s"""SELECT * FROM "$targetDomain"."$assertTable" EXCEPT $sql"""
        case None =>
          val columnNames = sql
          if (missing)
            s"""SELECT $columnNames FROM "$targetDomain"."$targetTable" EXCEPT SELECT * FROM "$targetDomain"."$assertTable""""
          else // not expected
            s"""SELECT * FROM "$targetDomain"."$assertTable" EXCEPT SELECT $columnNames FROM "$targetDomain"."$targetTable""""
      }
    selectStatement
  }

// Call transpile
  def outputTableDifferences(
    conn: Connection,
    targetDomain: String,
    targetTable: String,
    assertDatum: StarlakeTestData,
    outputPath: File,
    selectStatement: String
  ): Boolean = {
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

      val expectedColumns = assertDatum.expectationAsSql.getOrElse("*")
      val targetColumns =
        describe(conn, s"""SELECT $expectedColumns FROM "$targetDomain"."$targetTable"""")
      val assertColumns = describe(conn, s"$targetDomain.${assertTable}")
      val missingColumns = assertColumns.diff(targetColumns)

      // Compare schemas
      val notExpectedColumns = targetColumns.diff(assertColumns)
      var success = true
      if (missingColumns.nonEmpty) {
        println(s"Missing columns: ${missingColumns.mkString(", ")}")
      }
      if (notExpectedColumns.nonEmpty) {
        println(s"Not expected columns: ${notExpectedColumns.mkString(", ")}")
      }

      val notExpectedPath =
        new File(testFolder.jfile, s"not_expected${assertDatum.expectationName}.csv")
      val missingPath = new File(testFolder.jfile, s"missing${assertDatum.expectationName}.csv")
      if (missingColumns.isEmpty && notExpectedColumns.isEmpty) {
        // if schema OK,compare contents
        val missingSuccess = outputTableDifferences(
          conn,
          targetDomain,
          targetTable,
          assertDatum,
          missingPath,
          expectationQuery(targetDomain, targetTable, assertDatum, missing = true)
        )

        val notExpectedSuccess = outputTableDifferences(
          conn,
          targetDomain,
          assertTable,
          assertDatum,
          notExpectedPath,
          expectationQuery(targetDomain, targetTable, assertDatum, missing = false)
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

      val missingData = Files.readAllLines(missingPath.toPath).asScala.mkString("\n")
      val notExpectedData = Files.readAllLines(notExpectedPath.toPath).asScala.mkString("\n")
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
        exception = None,
        duration = duration
      )
    }
  }

  private def execute(conn: Connection, sql: String): Unit = {
    val stmt = conn.createStatement()
    println(sql)
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
    def runner(test: StarlakeTest, settings: Settings): Unit = {
      val params = Array("transform", "--test", "--name", test.name) ++ config.toArgs
      val testEnvPath =
        new Path(
          DatasetArea.tests(settings),
          s"transform/${test.domain}/${test.table}/${test.getTaskName()}/_env.sl.yml"
        )
      val storage = settings.storageHandler()
      val testEnvVars =
        EnvDesc
          .loadEnv(testEnvPath)(storage)
          .map(_.env)
          .getOrElse(Map.empty)

      val schemaHandler = new SchemaHandler(storage, testEnvVars)(settings)

      new Main().run(
        params,
        schemaHandler
      )(settings) match {
        case Failure(e) =>
          throw e
        case Success(_) =>
      }
    }

    val rootFolder =
      config.outputDir
        .flatMap(dir => {
          val file = new File(dir)
          if (file.exists() || file.mkdirs()) {
            Option(file)
          } else {
            Console.err.println(s"Could not create output directory $dir")
            None
          }
        })
        .getOrElse(new File(originalSettings.appConfig.root, "test-reports"))
    val testsFolder = new Directory(new File(rootFolder, "transform"))
    run(dataAndTests, runner, testsFolder)
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
    def runner(test: StarlakeTest, settings: Settings): Unit = {
      val tmpDir =
        test.incomingFiles.headOption.map { incomingFile =>
          val tmpDir = new Directory(new java.io.File(incomingFile.getParentFile, "tmp"))
          tmpDir.deleteRecursively()
          tmpDir.createDirectory(force = true)
          tmpDir.toFile
        }
      val params =
        Array("load", "--test", "--domains", test.domain, "--tables", test.table) ++
        Array(
          "--files",
          test.incomingFiles
            .flatMap { incomingFile =>
              tmpDir.map { tmpDir =>
                val tmpFile =
                  new File(tmpDir.toString(), incomingFile.getName.substring("_incoming.".length))
                Files.copy(incomingFile.toPath, tmpFile.toPath).toFile.toString
              }
            }
            .mkString(",")
        ) ++
        config.toArgs

      val testEnvPath =
        new Path(
          DatasetArea.tests(settings),
          s"load/${test.domain}/${test.table}/${test.getTaskName()}/_env.sl.yml"
        )
      val storage = settings.storageHandler()
      val testEnvVars =
        EnvDesc
          .loadEnv(testEnvPath)(storage)
          .map(_.env)
          .getOrElse(Map.empty)

      val schemaHandler = new SchemaHandler(storage, testEnvVars)(settings)
      new Main().run(
        params,
        schemaHandler
      )(settings) match {
        case Failure(e) =>
          tmpDir.foreach(_.deleteRecursively())
          throw e
        case Success(_) =>
          tmpDir.foreach(_.deleteRecursively())
      }

    }

    val rootFolder =
      config.outputDir
        .flatMap(dir => {
          val file = new File(dir)
          if (file.exists() || file.mkdirs()) {
            Option(file)
          } else {
            Console.err.println(s"Could not create output directory $dir")
            None
          }
        })
        .getOrElse(new File(originalSettings.appConfig.root, "test-reports"))
    val testsFolder = new Directory(new File(rootFolder, "load"))
    run(dataAndTests, runner, testsFolder)
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
      List[(String, String)] // Domain and Table names
    ),
    runner: (StarlakeTest, Settings) => Unit,
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
              if (test.incomingFiles.isEmpty) {
                Success(())
              } else {
                Try(runner(test, settings))
              }
            result match {
              case Failure(e) =>
                val end = System.currentTimeMillis()
                println(s"Test $domainName.$tableName.$testName failed to run (${e.getMessage})")
                Array(
                  StarlakeTestResult(
                    testFolder = testFolder.path,
                    domainName = domainName,
                    tableName = tableName,
                    taskName = test.getTaskName(),
                    testName = testName,
                    expectationName = "",
                    missingColumns = Nil,
                    notExpectedColumns = Nil,
                    missingRecords = new File(""),
                    notExpectedRecords = new File(""),
                    success = false,
                    exception = Some(Utils.exceptionAsString(e)),
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
                    test.expectations,
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

  def rootTestData(load: Boolean)(implicit settings: Settings): List[StarlakeTestData] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val testDir = new File(path.toString)
    Option(testDir.listFiles)
      .getOrElse(Array())
      .filter(_.isFile)
      .toList
      .flatMap(f => testDataFromCsvOrJsonFile("", "", f, "", ""))
  }

  def domainNames(load: Boolean)(implicit settings: Settings): List[String] =
    domainFolders(load).map(_.getName)

  def domainFolders(load: Boolean)(implicit settings: Settings): List[File] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val testDir = new File(path.toString)
    Option(testDir.listFiles)
      .getOrElse(Array())
      .filter(_.isDirectory)
      .toList
  }

  def taskOrTableFolders(domainFolder: File): List[File] =
    Option(domainFolder.listFiles(_.isDirectory)).map(_.toList).getOrElse(Nil)

  def taskOrTableNames(load: Boolean, domainName: String)(implicit
    settings: Settings
  ): List[String] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val domainFolder = new File(path.toString, domainName)
    taskOrTableFolders(domainFolder)
      .map(_.getName)
  }

  def testFolders(taskOrTableFolder: File): List[File] =
    Option(taskOrTableFolder.listFiles(_.isDirectory)).map(_.toList).getOrElse(Nil)

  def testFolders(load: Boolean, domainName: String, taskOrTableName: String)(implicit
    settings: Settings
  ): List[File] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val domainFolder = new File(path.toString, domainName)
    val taskOrTableFolder = new File(domainFolder, taskOrTableName)
    testFolders(taskOrTableFolder)
  }

  def testNames(load: Boolean, domainName: String, taskOrTableName: String)(implicit
    settings: Settings
  ): List[String] =
    testFolders(load, domainName, taskOrTableName).map(_.getName)

  def domainTestData(domainFolder: File)(implicit settings: Settings): List[StarlakeTestData] =
    Option(domainFolder.listFiles)
      .getOrElse(Array())
      .filter(_.isFile)
      .toList
      .flatMap(f => testDataFromCsvOrJsonFile("", "", f, domainFolder.getName, ""))

  def domainTestData(load: Boolean, domainName: String)(implicit
    settings: Settings
  ): List[StarlakeTestData] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val domainFolder = new File(path.toString, domainName)
    domainTestData(domainFolder)
  }

  def taskOrTableTestData(
    taskOrTableFolder: File,
    domainName: String,
    tableOrTaskName: String
  )(implicit settings: Settings): List[StarlakeTestData] =
    Option(taskOrTableFolder.listFiles)
      .getOrElse(Array())
      .filter(_.isFile)
      .toList
      .flatMap(f => testDataFromCsvOrJsonFile("", "", f, domainName, tableOrTaskName))

  def taskOrTableTestData(
    load: Boolean,
    domainName: String,
    taskOrTableName: String
  )(implicit settings: Settings): List[StarlakeTestData] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val domainFolder = new File(path.toString, domainName)
    val taskOrTableFolder = new File(domainFolder, taskOrTableName)
    taskOrTableTestData(taskOrTableFolder, domainName, taskOrTableName)
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
  def loadTests(
    load: Boolean,
    thisDomainName: String,
    thisTaskOrTableName: String,
    onlyThisTest: String
  )(implicit
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
    val schemaHandler = settings.schemaHandler()

    // Domain names we are willing to test
    val filteredDomainFolders =
      domainFolders(load).filter(domainDir =>
        thisDomainName.isEmpty || thisDomainName == domainDir.getName || thisDomainName == "*"
      )

    val allTests =
      filteredDomainFolders.map { domainFolder =>
        val filteredTaskOrTableFolders = taskOrTableFolders(domainFolder)
          .filter(
            thisTaskOrTableName.isEmpty || thisTaskOrTableName == _.getName || thisTaskOrTableName == "*"
          )

        val domainTests = filteredTaskOrTableFolders.map { taskOrTableFolder =>
          val filteredTestFolders =
            testFolders(taskOrTableFolder)
              .filter(
                onlyThisTest.isEmpty ||
                onlyThisTest == _.getName ||
                onlyThisTest == "*"
              )

          val taskOrTableTests: List[(String, StarlakeTest)] =
            filteredTestFolders
              .flatMap { testFolder =>
                loadTest(schemaHandler, testFolder)
                  .map { test =>
                    testFolder.getName -> test
                  }
              }

          taskOrTableFolder.getName -> (
            taskOrTableTestData(
              taskOrTableFolder,
              domainFolder.getName,
              taskOrTableFolder.getName
            ),
            taskOrTableTests
          )
        }
        domainFolder.getName -> (domainTestData(domainFolder), domainTests)
      }

    val domainAndTables =
      if (load)
        schemaHandler.domains().flatMap { dom =>
          dom.tables.map { table =>
            (dom.name, table.name)
          }
        }
      else
        schemaHandler.tasks().map { task =>
          (task.domain, task.name)
        }
    (rootTestData(load), allTests, domainAndTables)
  }

  def loadTest(
    schemaHandler: SchemaHandler,
    load: Boolean,
    domainName: String,
    taskOrTableName: String,
    testName: String
  )(implicit
    settings: Settings
  ): Option[StarlakeTest] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val domainFolder = new File(path.toString, domainName)
    val taskOrTableFolder = new File(domainFolder, taskOrTableName)
    val testFolder = new File(taskOrTableFolder, testName)
    val test = loadTest(schemaHandler, testFolder)
    test
  }

  def loadTest(
    schemaHandler: SchemaHandler,
    testFolder: File
  )(implicit
    settings: Settings
  ): Option[StarlakeTest] = {
    val taskOrTableFolderName = testFolder.getParentFile.getName
    val domainName = testFolder.getParentFile.getParentFile.getName
    // Al files that do not start with an '_' are considered data files
    // files that end with ".sql" are considered as tests to run against
    // the output and compared against the expected output in the "_expected_filename" file
    val testDataFiles = Option(
      testFolder
        .listFiles(f => f.isFile && !f.getName.startsWith("_expected"))
    )
      .map(_.toList)
      .getOrElse(Nil)

    // assert files start with an '_expected_' prefix

    val testExpectationsData = expectationsTestData(schemaHandler, testFolder)

    val table = schemaHandler.tableOnly(s"$domainName.$taskOrTableFolderName").toOption.map(_.table)

    val task =
      if (table.isDefined) None
      else schemaHandler.taskOnly(s"$domainName.$taskOrTableFolderName").toOption

    (table, task) match {
      case (Some(table), None) =>
        // handle load
        val preloadFiles =
          testDataFiles.filter { path =>
            val name = path.getName
            name.equals(s"$domainName.$taskOrTableFolderName.json") ||
            name.equals(s"$domainName.$taskOrTableFolderName.csv")
          }
        // if only one file with the table name is found. This means that we are loading in overwrite mode.
        // In this case we do not need the preload.
        // The file in that case designates the load path

        val preloadTestData = testDataFiles.flatMap { dataPath =>
          testDataFromCsvOrJsonFile(
            testFolder.getName,
            "",
            dataPath,
            domainName,
            taskOrTableFolderName
          )
        }

        // For extra files, we try to match them with the table name to detect invalid files
        val (matchingPatternData, unmatchingPatternData) =
          preloadTestData.filter(_.incoming).partition { incoming =>
            val name = incoming.filename
            table.pattern.matcher(name).matches()
          }
        if (unmatchingPatternData.nonEmpty) {
          val testName = testFolder.getName
          // scalastyle:off
          println(
            s"Load Test $domainName.$taskOrTableFolderName.$testName has unmatched load files: ${unmatchingPatternData
                .map(_.filename)
                .mkString(", ")}"
          )
        }

        val matchingPatternFiles =
          matchingPatternData.map(m => new File(testFolder, "_incoming." + m.filename))
        Some(
          StarlakeTest(
            s"$domainName.$taskOrTableFolderName",
            domainName,
            taskOrTableFolderName,
            testExpectationsData,
            preloadTestData,
            matchingPatternFiles
          )
        )
      case (None, Some(task)) =>
        // handle transform
        val testDataList =
          testDataFiles.flatMap { dataPath =>
            testDataFromCsvOrJsonFile(
              testFolder.getName,
              "",
              dataPath,
              domainName,
              taskOrTableFolderName
            )
          }
        Some(
          StarlakeTest(
            s"$domainName.$taskOrTableFolderName",
            task.domain,
            task.table,
            testExpectationsData,
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

  def expectationsTestData(
    schemaHandler: SchemaHandler,
    load: Boolean,
    domainName: String,
    taskOrTableName: String,
    testName: String
  )(implicit
    settings: Settings
  ): Array[StarlakeTestData] = {
    val path = if (load) DatasetArea.loadTests else DatasetArea.transformTests
    val domainFolder = new File(path.toString, domainName)
    val taskOrTableFolder = new File(domainFolder, taskOrTableName)
    val testFolder = new File(taskOrTableFolder, testName)
    expectationsTestData(schemaHandler, testFolder)
  }

  def expectationsTestData(
    schemaHandler: SchemaHandler,
    testFolder: File
  )(implicit settings: Settings): Array[StarlakeTestData] = {
    val domainName = testFolder.getParentFile.getParentFile.getName
    Option(testFolder.listFiles()).getOrElse(Array.empty).flatMap { f =>
      val filename = f.getName
      val isExpectationDataFile =
        f.isFile &&
        filename.startsWith("_expected") &&
        (filename.endsWith(".json") || f.getName.endsWith(".csv"))
      if (isExpectationDataFile) {
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
              expectationName,
              filename
            )
          )
        } else {
          val expectationSqlFile = new File(testFolder, s"_expected$expectationName.sql")
          if (expectationSqlFile.exists()) {
            val source = Source.fromFile(expectationSqlFile)
            val sql = source.mkString
            source.close()
            Some(
              StarlakeTestData(
                domainName,
                "sl_expected" + expectationName,
                expectedCreateTable,
                Some(sql),
                expectationName,
                filename
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
  }

  private def loadDataAsCreateTableExpression(
    schemaHandler: SchemaHandler,
    domainName: String,
    tableName: String,
    dataFile: File
  )(implicit settings: Settings): String = {

    val table = schemaHandler.tableOnly(s"$domainName.$tableName").map(_.table).toOption
    val expectedCreateTable = table match {
      case Some(table) if table.isFlat() =>
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
          else if (dataFile.getName.endsWith("csv"))
            s"(FORMAT CSV, nullstr '${settings.appConfig.testCsvNullString}')"
          else ""
        s"""CREATE OR REPLACE TABLE "$domainName"."$tableName" ($cols);
                 |COPY "$domainName"."$tableName" FROM '${dataFile.toString}' $extraArgs;""".stripMargin
      case Some(table) if !table.isFlat() =>
        // We have the table YML file, we thus create the table schema using the YML file ddl mapping feature
        s"""ERROR: Nested tables are not supported in tests => table $domainName.$table"""
      case None =>
        // Table not present in starlake schema, we let duckdb infer the schema
        val source =
          if (dataFile.getName.endsWith("csv"))
            s"read_csv('${dataFile.toString}', header = true, nullstr = '${schemaHandler.activeEnvVars().getOrElse("SL_CSV_NULLSTR", "null")}')"
          else s"'${dataFile.toString}'"
        s"CREATE OR REPLACE TABLE $domainName.$tableName AS SELECT * FROM $source;"
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
  private def testDataFromCsvOrJsonFile(
    testName: String,
    expectationName: String,
    dataPath: File,
    domainName: String,
    taskOrTableName: String
  )(implicit
    settings: Settings
  ): Option[StarlakeTestData] = {
    val dataName = dataPath.getName
    if (dataName.startsWith("_incoming.")) {
      Some(
        StarlakeTestData(
          domainName, // Schema name in DuckDB
          taskOrTableName, // Table name in DuckDB
          "", // csv/json content
          None,
          expectationName,
          dataPath.getName.substring("_incoming.".length),
          incoming = true
        )
      )
    } else {
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
                val schemaHandler = settings.schemaHandler()
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
              expectationName,
              dataPath.getName
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
}
