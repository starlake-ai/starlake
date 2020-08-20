package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.TestHelper

class BiqQueryLoadJobSpec extends TestHelper {
  new WithSettings() {
    "All BiqQueryLoad Config" should "be known and taken  into account" in {
      val rendered = BigQueryLoadConfig.usage()
      val expected =
        """
          |Usage: comet bqload [options]
          |
          |
          |  --source_file <value>    Full Path to source file
          |  --output_dataset <value>
          |                           BigQuery Output Dataset
          |  --output_table <value>   BigQuery Output Table
          |  --output_partition <value>
          |                           BigQuery Partition Field
          |  --require_partition_filter <value>
          |                           Require Partition Filter
          |  --output_clustering col1,col2...
          |                           BigQuery Clustering Fields
          |  --source_format <value>  Source Format eq. parquet
          |  --create_disposition <value>
          |                           Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition
          |  --write_disposition <value>
          |                           Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition
          |  --row_level_security <value>
          |                           value is in the form name,filter,sa:sa@mail.com,user:user@mail.com,group:group@mail.com
          |
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }
}
