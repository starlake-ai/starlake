package ai.starlake.job.sink.jdbc

import ai.starlake.TestHelper

class ConnectionLoadJobSpec extends TestHelper {
  new WithSettings {
    "All CnxLoad Config" should "be known and taken  into account" in {
      val rendered = ConnectionLoadConfig.usage()
      val expected =
        """
          |Usage: starlake cnxload [options]
          |Load parquet file into JDBC Table.
          |  --source_file <value>    Full Path to source file
          |  --output_table <value>   JDBC Output Table
          |  --options <value>        Connection options eq for jdbc:driver,user,password,url,partitions,batchSize
          |  --create_disposition <value>
          |                           Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition
          |  --write_disposition <value>
          |                           Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }
}
