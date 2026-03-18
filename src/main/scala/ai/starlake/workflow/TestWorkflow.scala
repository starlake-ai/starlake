package ai.starlake.workflow

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{EmptyJobResult, FailedJobResult, JobResult}
import ai.starlake.tests.{
  StarlakeTestConfig,
  StarlakeTestCoverage,
  StarlakeTestData,
  StarlakeTestResult
}
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.io.Directory
import scala.util.{Failure, Success}

trait TestWorkflow extends LazyLogging {
  this: IngestionWorkflow =>

  protected def schemaHandler: SchemaHandler
  implicit protected def settings: Settings

  private def testsLog(transformResults: List[StarlakeTestResult]): JobResult = {
    val (success, failure) = transformResults.partition(_.success)
    println(s"Tests run: ${transformResults.size} ")
    println(s"Tests succeeded: ${success.size}")
    println(s"Tests failed: ${failure.size}")
    if (failure.nonEmpty) {
      println(
        s"Tests failed: ${failure.map { t => s"${t.domainName}.${t.taskName}.${t.testName}" }.mkString("\n")}"
      )
    }
    if (success.nonEmpty) {
      println(
        s"Tests succeeded: ${success.map { t => s"${t.domainName}.${t.taskName}.${t.testName}" }.mkString("\n")}"
      )
    }
    if (failure.nonEmpty) {
      FailedJobResult
    } else {
      EmptyJobResult
    }
  }

  def testLoad(config: StarlakeTestConfig): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    val domainName = config.domain
    val tableName = config.table
    val expectations =
      (domainName, tableName) match {
        case (Some(d), Some(t)) =>
          schemaHandler.tableOnly(s"$d.$t") match {
            case Success(tableDesc) => tableDesc.table.expectations
            case Failure(_)         => Nil
          }
        case _ => // all good
          Nil
      }
    val loadTests = StarlakeTestData.loadTests(
      load = true,
      config.domain.getOrElse(""),
      config.table.getOrElse(""),
      config.test.getOrElse("")
    )
    StarlakeTestData.runLoads(loadTests, config)
  }

  def testTransform(
    config: StarlakeTestConfig
  ): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    val domainName = config.domain
    val tableName = config.table
    val expectations =
      (domainName, tableName) match {
        case (Some(d), Some(t)) =>
          schemaHandler.taskByFullName(s"$d.$t") match {
            case Success(taskInfo) => taskInfo.expectations
            case Failure(_)        => Nil
          }
        case _ => // all good
          Nil
      }
    val transformTests = StarlakeTestData.loadTests(
      load = false,
      config.domain.getOrElse(""),
      config.table.getOrElse(""),
      config.test.getOrElse("")
    )
    StarlakeTestData.runTransforms(transformTests, config)
  }

  def testLoadAndTransform(
    config: StarlakeTestConfig
  ): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    val loadResults =
      if (config.runLoad()) {
        testLoad(config)
      } else
        (Nil, StarlakeTestCoverage(Set.empty, Set.empty, Nil, Nil))
    val transformResults =
      if (config.runTransform()) {
        testTransform(config)
      } else
        (Nil, StarlakeTestCoverage(Set.empty, Set.empty, Nil, Nil))
    if (config.generate) {
      StarlakeTestResult.html(loadResults, transformResults, config.outputDir)
      testsLog(loadResults._1 ++ transformResults._1)
    } else {
      import java.io.File as JFile
      val rootFolder =
        config.outputDir
          .flatMap { dir =>
            val file = new JFile(dir)
            if (file.exists() || file.mkdirs()) {
              Option(new Directory(file))
            } else {
              Console.err.println(s"Could not create output directory $dir")
              None
            }
          }
          .getOrElse(new Directory(new JFile(settings.appConfig.root, "test-reports")))
      StarlakeTestResult.junitXml(loadResults._1, transformResults._1, rootFolder)
    }
    (loadResults._1 ++ transformResults._1, loadResults._2.merge(transformResults._2))
  }
}