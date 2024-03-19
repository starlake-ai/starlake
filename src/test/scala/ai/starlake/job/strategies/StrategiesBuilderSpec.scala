package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AllSinks, MergeOn, WriteStrategy, WriteStrategyType}

trait StrategiesBuilderSpec extends TestHelper {
  def engine: String
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "CREATE"
        )
      logger.info("------------------------------------------------------------")
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "CREATE"
        )
      logger.info("------------------------------------------------------------")
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "CREATE"
        )
      logger.info("------------------------------------------------------------")
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "APPEND"
        )
      logger.info("------------------------------------------------------------")
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "APPEND"
        )
      logger.info("------------------------------------------------------------")
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "OVERWRITE"
        )
      logger.info("------------------------------------------------------------")
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY"
        )
      logger.info("------------------------------------------------------------")
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY"
        )
      logger.info("------------------------------------------------------------")
    }

    "upsert by key and timestamp on target" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
          key = List("transaction_id"),
          on = Some(MergeOn.TARGET),
          timestamp = Some("transaction_date"),
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY_AND_TIMESTAMP"
        )
      logger.info("------------------------------------------------------------")
    }

    "upsert by key and timestamp on source and target" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
          key = List("transaction_id"),
          on = Some(MergeOn.SOURCE_AND_TARGET),
          timestamp = Some("transaction_date"),
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
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY_AND_TIMESTAMP"
        )
      logger.info("------------------------------------------------------------")
    }

  }
}
