package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{AutoTaskDesc, BigQuerySink, WriteMode}
import ai.starlake.workflow.IngestionWorkflow
import com.google.cloud.bigquery.{BigQueryOptions, StandardTableDefinition, Table, TableId}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class BigQuerySparkJobSpec extends TestHelper with BeforeAndAfterAll {
  private val bigquery = BigQueryOptions.newBuilder().build().getService
  override def beforeAll(): Unit = {
    super.beforeAll()
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("SL_BQ_TEST_DS", "SL_BQ_TEST_TABLE_DYNAMIC"))
    }
  }
  override def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
    }
  }
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    it should "overwrite partitions dynamically" in {

      val bigQueryConfiguration: Config = {
        val config = ConfigFactory.parseString("""
                                                 |connectionRef = "sparkbq"
                                                 |
                                                 |connections.sparkbq {
                                                 |  sparkFormat = "bigquery"
                                                 |  type = "bigquery"
                                                 |  options {
                                                 |    gcsBucket: starlake-app
                                                 |    writeMethod: indirect
                                                 |    location: "europe-west1"
                                                 |    authType: APPLICATION_DEFAULT
                                                 |    #authType: SERVICE_ACCOUNT_JSON_KEYFILE
                                                 |    #jsonKeyfile: "/Users/me/.gcloud/keys/my-key.json"
                                                 |  }
                                                 |}
                                                 |
                                                 |""".stripMargin)
        val result = config.withFallback(super.testConfiguration)
        result
      }
      new WithSettings(bigQueryConfiguration) {
        new SpecTrait(
          jobFilename = Some("_config.sl.yml"),
          sourceDomainOrJobPathname = "/sample/tableWithPartitions/_config.sl.yml",
          datasetDomainName = "SL_BQ_TEST_DS",
          sourceDatasetPathName = ""
        ) {
          cleanMetadata
          cleanDatasets
          private val query: String =
            """
              |        WITH tbl as (
              |          select "joe" as name,Date("2022-01-01") as dob
              |          union all
              |          select "sam" as name, Date("2023-02-01") as dob
              |        )
              |        select * from tbl
              |""".stripMargin
          private val businessTaskPart = AutoTaskDesc(
            "tableWithPartitions",
            None,
            None,
            "SL_BQ_TEST_DS",
            "SL_BQ_TEST_TABLE_DYNAMIC",
            Some(WriteMode.OVERWRITE),
            sink = Some(
              BigQuerySink(connectionRef = None, timestamp = Some("DOB")).toAllSinks()
            ),
            merge = None
          )

          case class Task(task: AutoTaskDesc)

          private val businessTaskPartDef = mapper
            .writer()
            .withAttribute(classOf[Settings], settings)
            .writeValueAsString(Task(businessTaskPart))
          val pathTask =
            new Path(
              jobMetadataRootPath,
              "SL_BQ_TEST_DS/tableWithPartitions.sl.yml"
            )
          val pathTaskSQL =
            new Path(
              jobMetadataRootPath,
              "SL_BQ_TEST_DS/tableWithPartitions.sql.j2"
            )
          storageHandler.mkdirs(pathTask.getParent)
          storageHandler.write(businessTaskPartDef, pathTask)
          storageHandler.write(query, pathTaskSQL)

          private val queryAdd: String =
            """
              |WITH tbl as (
              |  select "sam" as name,Date("1990-01-01") as dob
              |  union all
              |  select "joe" as name, Date("1992-02-01") as dob
              |)
              |select * from tbl
              |""".stripMargin
          private val businessTaskAddPart = AutoTaskDesc(
            "addPartitionsWithOverwrite",
            Some(query),
            None,
            "SL_BQ_TEST_DS",
            "SL_BQ_TEST_TABLE_DYNAMIC",
            Some(WriteMode.OVERWRITE),
            sink = Some(
              BigQuerySink(connectionRef = None, timestamp = Some("DOB")).toAllSinks()
            ),
            merge = None
          )
          private val businessTaskAddPartDef = mapper
            .writer()
            .withAttribute(classOf[Settings], settings)
            .writeValueAsString(Task(businessTaskAddPart))
          val pathTaskAdd =
            new Path(
              this.jobMetadataRootPath,
              "SL_BQ_TEST_DS/addPartitionsWithOverwrite.sl.yml"
            )
          val pathTaskSQLAdd =
            new Path(
              this.jobMetadataRootPath,
              "SL_BQ_TEST_DS/addPartitionsWithOverwrite.sql.j2"
            )
          storageHandler.write(businessTaskAddPartDef, pathTaskAdd)
          storageHandler.write(queryAdd, pathTaskSQLAdd)
          val schemaHandler = new SchemaHandler(storageHandler)
          logger.info("Job:SL_BQ_TEST_DS")
          logger.info(pathTaskSQL.toString)
          logger.info(this.jobMetadataRootPath.toString)

          schemaHandler.jobs(true).foreach(it => logger.info(it.toString))
          val validator = new IngestionWorkflow(storageHandler, schemaHandler)
          validator
            .autoJob(TransformConfig("SL_BQ_TEST_DS.tableWithPartitions"))
            .isSuccess shouldBe true
          // check that table is created correctly with the right number of lines
          private val createdTable: Table =
            bigquery.getTable(TableId.of("SL_BQ_TEST_DS", "SL_BQ_TEST_TABLE_DYNAMIC"))
          createdTable.getNumRows.intValue() shouldBe 2
          createdTable
            .getDefinition[StandardTableDefinition]
            .getTimePartitioning
            .getField
            .shouldBe("DOB")
          validator
            .autoJob(
              TransformConfig("SL_BQ_TEST_DS.addPartitionsWithOverwrite")
            )
            .isSuccess shouldBe true
          private val updatedTable: Table =
            bigquery.getTable(TableId.of("SL_BQ_TEST_DS", "SL_BQ_TEST_TABLE_DYNAMIC"))
          // check that table is appended with new partitions
          updatedTable.getNumRows.intValue() shouldBe 4
        }
      }
    }
  }
}
