package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AllSinks, Engine, MergeOn, WriteStrategy, WriteStrategyType}

class BigQueryStrategiesBuilderSpec extends TestHelper {
  new WithSettings() {
    "create table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
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
    "create table strategy source and target" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND),
          on = Some(MergeOn.SOURCE_AND_TARGET),
          key = List("transaction_id")
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
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
    }
    "create table  strategy source and target with SCD2" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.SCD2),
          key = List("transaction_id"),
          on = Some(MergeOn.SOURCE_AND_TARGET),
          startTs = Some("start_ts"),
          endTs = Some("end_ts")
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
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
    "append table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = false,
          settings.appConfig.jdbcEngines("bigquery"),
          AllSinks().getSink(),
          Engine.fromString("bigquery"),
          "APPEND"
        )
      println(finalSql)
    }

    "truncate then append table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = true,
          materializedView = false,
          settings.appConfig.jdbcEngines("bigquery"),
          AllSinks().getSink(),
          Engine.fromString("bigquery"),
          "APPEND"
        )
      println(finalSql)
    }

    "overwrite table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = true,
          materializedView = false,
          settings.appConfig.jdbcEngines("bigquery"),
          AllSinks().getSink(),
          Engine.fromString("bigquery"),
          "OVERWRITE"
        )
      println(finalSql)
    }
    "upsert by key" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.UPSERT_BY_KEY),
          key = List("transaction_id"),
          on = Some(MergeOn.TARGET),
          startTs = Some("start_ts"),
          endTs = Some("end_ts")
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = false,
          settings.appConfig.jdbcEngines("bigquery"),
          AllSinks().getSink(),
          Engine.fromString("bigquery"),
          "UPSERT_BY_KEY"
        )
    }

    "upsert by key on source and target" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.UPSERT_BY_KEY),
          key = List("transaction_id"),
          on = Some(MergeOn.SOURCE_AND_TARGET),
          startTs = Some("start_ts"),
          endTs = Some("end_ts")
        )

      val finalSql =
        new BigQueryStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "SELECT '12345' as transaction_id, '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info",
          StrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = false,
          settings.appConfig.jdbcEngines("bigquery"),
          AllSinks().getSink(),
          Engine.fromString("bigquery"),
          "UPSERT_BY_KEY"
        )
    }

  }
}
