package ai.starlake.integration

import com.google.cloud.bigquery.{BigQueryOptions, TableId}

class BigQueryIntegrationSpecBase extends IntegrationTestBase {

  val bigquery = BigQueryOptions.newBuilder().build().getService()

  /** We delete the table before running the test to ensure that the test is run in a clean
    * environment. We do not delete the table after afterwards because we want to be able to inspect
    * the table after the test.
    */
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("sales", "customers"))
      bigquery.delete(TableId.of("sales", "orders"))
      bigquery.delete(TableId.of("hr", "sellers"))
      bigquery.delete(TableId.of("hr", "locations"))
    }
  }
}
