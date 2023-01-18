package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import com.google.cloud.bigquery.TableId

class BigQuerySparkJobSpec extends TestHelper {
  it should "get table with a dataset name including project" in {
    val tableId = BigQuerySparkJob.getTableId(sparkSession, "my-project:my-dataset.my-table")
    tableId shouldBe TableId.of("my-project", "my-dataset", "my-table")
  }

  it should "get table with default project id when dataset name doesn't include projectId" in {
    val tableId = BigQuerySparkJob.getTableId(sparkSession, "my-dataset.my-table")
    tableId.getDataset shouldBe "my-dataset"
    tableId.getTable shouldBe "my-table"
  }

  it should "get table with default project id specified in spark conf when dataset name doesn't include projectId" in {
    sparkSession.sparkContext.hadoopConfiguration.set("fs.gs.project.id", "my-project")
    val tableId = BigQuerySparkJob.getTableId(sparkSession, "my-dataset.my-table")
    tableId shouldBe TableId.of("my-project", "my-dataset", "my-table")
  }
}
