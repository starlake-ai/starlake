package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.metrics.ExpectationAssertionHandler
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AutoTaskInfo, BigQuerySink, ExpectationItem, WriteStrategy}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.ListBuffer

/** Expectations of a transform run on a data branch must check the branch copy of the target, not
  * the production table. No BigQuery access: the assertion handler only records the SQL it gets.
  */
class BigQueryBranchExpectationsSpec extends TestHelper {

  private def bigQueryConfiguration(branch: Option[String]): Config = {
    val branchOption = branch.map(b => s"""sl_data_branch: "$b"""").getOrElse("")
    ConfigFactory
      .parseString(s"""
                      |connectionRef = "bq"
                      |connections.bq {
                      |  type = "bigquery"
                      |  options {
                      |    location: "europe-west1"
                      |    authType: APPLICATION_DEFAULT
                      |    $branchOption
                      |  }
                      |}
                      |""".stripMargin)
      .withFallback(super.testConfiguration)
  }

  private def expectationSql(branch: Option[String]): List[String] = {
    var captured: List[String] = Nil
    new WithSettings(bigQueryConfiguration(branch)) {
      deliverTypesFiles() // also installs the sample expectation macros
      implicit val sh: StorageHandler = storageHandler
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      val taskDesc = AutoTaskInfo(
        name = "order_summary",
        sql = Some("SELECT id FROM sales.orders"),
        database = Some("prj"),
        domain = "kpi",
        table = "order_summary",
        connectionRef = Some("bq"),
        sink = Some(BigQuerySink(connectionRef = Some("bq")).toAllSinks()),
        writeStrategy = Some(WriteStrategy.Overwrite),
        expectations = List(ExpectationItem("is_col_value_not_unique('id')"))
      )
      val sqls = ListBuffer[String]()
      val task = new BigQueryAutoTask(
        appId = None,
        taskDesc = taskDesc,
        commandParameters = Map.empty,
        interactive = None,
        truncate = false,
        test = false,
        logExecution = false,
        resultPageSize = 1,
        resultPageNumber = 1,
        dryRun = false,
        scheduledDate = None,
        syncSchema = false
      ) {
        override protected def expectationAssertionHandler: ExpectationAssertionHandler =
          new ExpectationAssertionHandler {
            override def handle(sql: String)(implicit settings: Settings): Int = {
              sqls += sql
              0
            }
          }
      }
      task.runExpectations().map(_.success) shouldBe List(true)
      captured = sqls.toList
    }
    captured
  }

  "BigQuery expectations on a data branch" should "query the branch copy of the target" in {
    val sqls = expectationSql(Some("sl_branch"))
    sqls should have size 1
    sqls.head should include("prj.sl_branch.order_summary")
    sqls.head should not include "kpi.order_summary"
  }

  "BigQuery expectations without a data branch" should "query the target table" in {
    val sqls = expectationSql(None)
    sqls should have size 1
    sqls.head should include("prj.kpi.order_summary")
  }
}
