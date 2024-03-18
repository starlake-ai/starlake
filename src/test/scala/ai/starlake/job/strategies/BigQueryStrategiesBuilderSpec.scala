package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AllSinks, Engine, WriteStrategy, WriteStrategyType}

class BigQueryStrategiesBuilderSpec extends TestHelper {
  new WithSettings() {
    "Append" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT transaction_id, transaction_date, amount, location_info, seller_info FROM starlake-project-id.dataset3.transactions_v3 from starlake-project-id.dataset3.transactions_v3",
          StrategiesBuilder.TableComponents(
            "starlake-project-id",
            "dataset3",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = false,
          settings.appConfig.jdbcEngines("bigquery"),
          AllSinks().getSink(),
          Engine.fromString("bigquery"),
          "CREATE"
        )
      println(finalSql)
    }

  }
}
