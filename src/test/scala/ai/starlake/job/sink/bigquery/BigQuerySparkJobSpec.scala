package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import com.google.cloud.bigquery.TableId

// TODO
class BigQuerySparkJobSpec extends TestHelper {
  if (false && sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
    it should "get table with a dataset name including project" in {
      val tableMetadata = BigQuerySparkJob.getTable("my-project:my-dataset.my-table")
      tableMetadata.table.get.getTableId shouldBe TableId.of("my-project", "my-dataset", "my-table")
    }

    it should "get table with default project id when dataset name doesn't include projectId" in {
      val tableMetadata = BigQuerySparkJob.getTable("my-dataset.my-table")
      tableMetadata.table.get.getTableId.getDataset shouldBe "my-dataset"
      tableMetadata.table.get.getTableId.getTable shouldBe "my-table"
    }
  }
}
