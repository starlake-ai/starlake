package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.schema.model.{AutoJobDesc, AutoTaskDesc, BigQuerySink, WriteMode}
import ai.starlake.workflow.IngestionWorkflow
import com.google.cloud.bigquery.{BigQueryOptions, StandardTableDefinition, Table, TableId}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class BigQuerySparkJobSpec extends TestHelper with BeforeAndAfterAll {
  private val bigquery = BigQueryOptions.newBuilder().build().getService
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("SL_BQ_TEST_DS", "SL_BQ_TEST_TABLE_DYNAMIC"))
    }
  }
  override def afterAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
    }
  }
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    // TODO
    // import com.google.cloud.bigquery.TableId
    /*
    it should "get table with a dataset name including project" in {
      val tableMetadata = BigQuerySparkJob.getTable("my-project:my-dataset.my-table")
      tableMetadata.table.get.getTableId shouldBe TableId.of("my-project", "my-dataset", "my-table")
    }

    it should "get table with default project id when dataset name doesn't include projectId" in {
      val tableMetadata = BigQuerySparkJob.getTable("my-dataset.my-table")
      tableMetadata.table.get.getTableId.getDataset shouldBe "my-dataset"
      tableMetadata.table.get.getTableId.getTable shouldBe "my-table"
    }*/
    it should "overwrite partitions dynamically" in {
      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "tableWithPartitions.comet.yml",
          sourceDomainOrJobPathname = "/sample/bq-integration-tests/tableWithPartitions.comet.yml",
          datasetDomainName = "SL_BQ_TEST_DS",
          sourceDatasetPathName = "",
          isDomain = false
        ) {
          private val query: String =
            """
              |WITH _table as (
              |  select "sam" as name,Date("1990-01-01") as dob
              |  union all
              |  select "joe" as name, Date("1992-02-01") as dob
              |)
              |select * from _table
              |""".stripMargin
          private val businessTask1 = AutoTaskDesc(
            "",
            Some(query),
            None,
            "SL_BQ_TEST_DS",
            "SL_BQ_TEST_TABLE_DYNAMIC",
            WriteMode.OVERWRITE,
            sink = Some(
              BigQuerySink(connectionRef = None, timestamp = Some("DOB"))
            ),
            merge = None
          )
          private val businessJob =
            AutoJobDesc(
              "addPartitionsWithOverwrite",
              List(businessTask1),
              Nil,
              None,
              None,
              None
            )
          private val businessJobDef = mapper
            .writer()
            .withAttribute(classOf[Settings], settings)
            .writeValueAsString(businessJob)
          cleanMetadata
          cleanDatasets
          val pathJob =
            new Path(cometMetadataPath + "/jobs/addPartitionsWithOverwrite.comet.yml")
          storageHandler.write(businessJobDef, pathJob)
          val schemaHandler = new SchemaHandler(storageHandler)
          val validator = new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
          validator.autoJob(TransformConfig("tableWithPartitions")) shouldBe true
          // check that table is created correctly with the right number of lines
          private val createdTable: Table =
            bigquery.getTable(TableId.of("SL_BQ_TEST_DS", "SL_BQ_TEST_TABLE_DYNAMIC"))
          createdTable.getNumRows.intValue() shouldBe 2
          createdTable
            .getDefinition[StandardTableDefinition]
            .getTimePartitioning
            .getField
            .shouldBe("DOB")
          validator.autoJob(TransformConfig("addPartitionsWithOverwrite")) shouldBe true
          private val updatedTable: Table =
            bigquery.getTable(TableId.of("SL_BQ_TEST_DS", "SL_BQ_TEST_TABLE_DYNAMIC"))
          // check that table is appended with new partitions
          updatedTable.getNumRows.intValue() shouldBe 4
        }
      }
    }
  }
}
