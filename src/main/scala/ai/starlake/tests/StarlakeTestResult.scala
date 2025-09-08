package ai.starlake.tests

import ai.starlake.config.Settings
import ai.starlake.utils.Utils

import java.io.File
import java.nio.file.Files
import java.time.format.DateTimeFormatter
import java.util
import scala.jdk.CollectionConverters._
import scala.reflect.io.Directory

case class JUnitTestSuite(
  name: String,
  tests: Int,
  failures: Int,
  errors: Int,
  time: Double,
  testCases: List[JunitTestCase]
) {
  def getName() = name
  def getTests(): Int = tests
  def getFailures(): Int = failures
  def getErrors(): Int = errors
  def getTime(): Double = time
  def getTestCases(): util.List[JunitTestCase] = testCases.asJava
}

case class JunitTestSuites(
  tests: Int,
  failures: Int,
  errors: Int,
  time: Double,
  loadSuite: JUnitTestSuite,
  transformSuite: JUnitTestSuite
) {
  def getTests(): Int = tests
  def getFailures(): Int = failures
  def getErrors(): Int = errors
  def getTime(): Double = time
  def getLoadSuite(): JUnitTestSuite = loadSuite
  def getTransformSuite(): JUnitTestSuite = transformSuite
  def getTestSuites(): util.List[JUnitTestSuite] = List(loadSuite, transformSuite).asJava
}

case class JunitTestCase(
  name: String,
  classname: String,
  time: Double,
  failure: Option[String] = None
) {
  def getName(): String = name
  def getClassname(): String = classname
  def getTime(): Double = time
  def getFailure(): String = failure.getOrElse("")
}

case class StarlakeTestResult(
  testFolder: String,
  domainName: String,
  tableName: String,
  taskName: String,
  testName: String,
  expectationName: String,
  missingColumns: List[String],
  notExpectedColumns: List[String],
  missingRecords: Option[File],
  notExpectedRecords: Option[File],
  success: Boolean,
  exception: Option[String],
  duration: Long,
  sql: Option[String]
) {
  def junitErrMessage(): Option[String] = {
    if (!success) {
      Some(
        s"""Missing columns: ${getMissingColumnsCount()}, Not expected columns: ${getNotExpectedColumnsCount()}, Missing records: ${getMissingRecordsCount()}, Not expected records: ${getNotExpectedRecordsCount()}, Exception: ${getExceptionHead()}""".stripMargin
      )
    } else None
  }
  def tName = if (Option(tableName).isEmpty || tableName.isEmpty) taskName else tableName
  def toJunitTestCase(): JunitTestCase = {
    val name: String = s"$domainName.$tName.$testName"
    val classname: String = s"$domainName.$taskName"
    val time: Double = duration.toDouble / 1000
    val failure: Option[String] = junitErrMessage()
    JunitTestCase(name, classname, time, failure)
  }
  // getters for jinjava
  def getTestFolder(): String = testFolder
  def getDomainName(): String = domainName
  def getTableName(): String = tableName
  def getTaskName(): String = taskName
  def getTestName(): String = testName
  def getExpectationName(): String =
    if (expectationName.isEmpty) "default" else expectationName.substring(1)
  def getMissingColumns(): java.util.List[String] = missingColumns.asJava
  def getMissingColumnsCount(): Int = missingColumns.size
  def getNotExpectedColumns(): java.util.List[String] = notExpectedColumns.asJava
  def getNotExpectedColumnsCount(): Int = notExpectedColumns.size
  def getMissingRecords(): String =
    missingRecords match {
      case Some(missingRecords) if missingRecords.exists() =>
        Files.readAllLines(missingRecords.toPath).asScala.mkString("\n")
      case _ => ""
    }

  def getMissingRecordsCount() =
    missingRecords match {
      case Some(missingRecords) if missingRecords.exists() =>
        val nbLines = getMissingRecords().split("\n").length
        if (nbLines >= 1) nbLines - 1 else 0
      case _ => 0
    }

  def getNotExpectedRecords(): String =
    notExpectedRecords match {
      case Some(notExpectedRecords) if notExpectedRecords.exists() =>
        Files.readAllLines(notExpectedRecords.toPath).asScala.mkString("\n")
      case _ => ""
    }

  def getNotExpectedRecordsCount() =
    notExpectedRecords match {
      case Some(notExpectedRecords) if notExpectedRecords.exists() =>
        val nbLines = getNotExpectedRecords().split("\n").length
        if (nbLines >= 1) nbLines - 1 else 0
      case _ => 0
    }

  def getSuccess(): Boolean = success
  def getExceptionHead(): String =
    exception.getOrElse("None").split("\n").head
  def getException(): util.List[String] =
    exception.getOrElse("None").split("\n").toList.asJava
  def getDuration(): String = {
    val d: Double = duration.toDouble / 1000
    s"$d"
  }

}

object StarlakeTestResult {
  val loader = new StarlakeTestTemplateLoader()

  def copyCssAndJs(toFolder: Directory)(implicit settings: Settings): Unit = {
    val cssAndJs = Array("css/base-style.css", "css/style.css", "js/report.js")
    cssAndJs.foreach { cj =>
      val content = loader.loadTemplate(s"$cj.j2")
      val targetFile = new File(toFolder.path, cj)
      targetFile.getParentFile().mkdirs()
      Files.write(targetFile.toPath, content.getBytes())
    }
  }
  def toJunitTestSuites(
    loadResults: List[StarlakeTestResult],
    transformResults: List[StarlakeTestResult]
  ): JunitTestSuites = {
    val loadTestCases = loadResults.map(_.toJunitTestCase())
    val transformTestCases = transformResults.map(_.toJunitTestCase())
    val loadSuite = JUnitTestSuite(
      name = "Tests.Load",
      tests = loadTestCases.size,
      failures = loadTestCases.count(_.failure.isDefined),
      errors = 0,
      time = loadTestCases.map(_.time).sum,
      testCases = loadTestCases
    )
    val transformSuite = JUnitTestSuite(
      name = "Tests.Transform",
      tests = transformTestCases.size,
      failures = transformTestCases.count(_.failure.isDefined),
      errors = 0,
      time = transformTestCases.map(_.time).sum,
      testCases = transformTestCases
    )
    JunitTestSuites(
      tests = loadSuite.tests + transformSuite.tests,
      failures = loadSuite.failures + transformSuite.failures,
      errors = 0,
      time = loadSuite.time + transformSuite.time,
      loadSuite = loadSuite,
      transformSuite = transformSuite
    )
  }
  def junitXml(
    loadResults: List[StarlakeTestResult],
    transformResults: List[StarlakeTestResult],
    rootFolder: Directory
  )(implicit settings: Settings): Unit = {

    val junitTestSuites = toJunitTestSuites(loadResults, transformResults)
    val j2Params = Map(
      "junitTestSuites" -> junitTestSuites,
      "timestamp"       -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
    )
    val indexJ2 = loader.loadTemplate("junit.xml.j2")
    val indexContent = Utils.parseJinja(indexJ2, j2Params)
    Files.write(new File(rootFolder.path, "junit.xml").toPath, indexContent.getBytes())

  }

  def html(
    loadAndCoverageResults: (List[StarlakeTestResult], StarlakeTestCoverage),
    transformAndCoverageResults: (List[StarlakeTestResult], StarlakeTestCoverage),
    outputDir: Option[String]
  )(implicit settings: Settings): Unit = {
    val rootFolder =
      outputDir
        .flatMap { dir =>
          val file = new File(dir)
          if (file.exists() || file.mkdirs()) {
            Option(new Directory(file))
          } else {
            Console.err.println(s"Could not create output directory $dir")
            None
          }
        }
        .getOrElse(new Directory(new File(settings.appConfig.root, "test-reports")))
    copyCssAndJs(rootFolder)

    val (loadResults, loadCoverage) = loadAndCoverageResults
    val loadSummaries = StarlakeTestsDomainSummary.summaries(loadResults)
    val loadIndex = StarlakeTestsSummary.summaryIndex(loadSummaries)

    val (transformResults, transformCoverage) = transformAndCoverageResults
    val transformSummaries = StarlakeTestsDomainSummary.summaries(transformResults)
    val transformIndex = StarlakeTestsSummary.summaryIndex(transformSummaries)
    val j2Params = Map(
      "loadIndex"          -> loadIndex,
      "loadSummaries"      -> loadSummaries.asJava,
      "transformIndex"     -> transformIndex,
      "transformSummaries" -> transformSummaries.asJava,
      "timestamp"          -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()),
      "loadCoverage"       -> loadCoverage,
      "transformCoverage"  -> transformCoverage
    )
    val indexJ2 = loader.loadTemplate("root.html.j2")
    val indexContent = Utils.parseJinja(indexJ2, j2Params)
    Files.write(new File(rootFolder.path, "index.html").toPath, indexContent.getBytes())

    val loadFolder = new Directory(new File(rootFolder.jfile, "load"))
    loadFolder.createDirectory()
    html(loadResults, loadFolder, "Load")
    val transformFolder = new Directory(new File(rootFolder.jfile, "transform"))
    transformFolder.createDirectory()
    html(transformResults, transformFolder, "Transform")
    junitXml(loadResults, transformResults, rootFolder)
  }

  def html(
    results: List[StarlakeTestResult],
    testsFolder: Directory,
    loadOrTransform: String
  )(implicit
    settings: Settings
  ): Unit = {
    val domainSummaries = StarlakeTestsDomainSummary.summaries(results)
    val summaryIndex = StarlakeTestsSummary.summaryIndex(domainSummaries)

    val j2Params = Map(
      "summaryIndex"    -> summaryIndex,
      "domainSummaries" -> domainSummaries.asJava,
      "timestamp"       -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()),
      "loadOrTransform" -> loadOrTransform
    )
    val indexJ2 = loader.loadTemplate("index.html.j2")
    val indexContent = Utils.parseJinja(indexJ2, j2Params)
    Files.write(new File(testsFolder.path, "index.html").toPath, indexContent.getBytes())

    domainSummaries.foreach { domainSummary =>
      val tableSummaries = StarlakeTestsTableSummary.summaries(domainSummary.name, results)
      val indexJ2 = loader.loadTemplate("index.domain.html.j2")
      val j2Params = Map(
        "domainSummary"   -> domainSummary,
        "tableSummaries"  -> tableSummaries.asJava,
        "timestamp"       -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()),
        "loadOrTransform" -> loadOrTransform
      )
      val domainFolder = new File(testsFolder.path, domainSummary.name)
      domainFolder.mkdir()
      val result = Utils.parseJinja(indexJ2, j2Params)
      Files.write(new File(domainFolder, "index.html").toPath, result.getBytes())

      tableSummaries.foreach { tableSummary =>
        val tableResults =
          results.filter(r => s"${r.domainName}.${r.taskName}" == tableSummary.name)
        val indexJ2 = loader.loadTemplate("index.table.html.j2")
        val j2Params = Map(
          "domainName"      -> domainSummary.name,
          "tableSummary"    -> tableSummary,
          "testResults"     -> tableResults.asJava,
          "timestamp"       -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()),
          "loadOrTransform" -> loadOrTransform
        )
        val tableFolder = new File(domainFolder, tableSummary.getTableName())
        tableFolder.mkdir()
        val result = Utils.parseJinja(indexJ2, j2Params)
        Files.write(new File(tableFolder, "index.html").toPath, result.getBytes())
      }
    }
    results.foreach { result =>
      val indexJ2 = loader.loadTemplate("index.test.html.j2")
      val j2Params = Map(
        "domainName"      -> result.domainName,
        "tableName"       -> result.taskName,
        "testResult"      -> result,
        "timestamp"       -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()),
        "loadOrTransform" -> loadOrTransform
      )
      val testFolder =
        new File(
          testsFolder.path,
          result.domainName + File.separator + result.taskName + File.separator + result.testName
        )
      testFolder.mkdir()
      val resultContent = Utils.parseJinja(indexJ2, j2Params)
      Files.write(
        new File(testFolder, s"${result.getExpectationName()}.html").toPath,
        resultContent.getBytes()
      )
    }

  }
}
