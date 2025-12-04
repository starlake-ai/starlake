package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model.{
  AllSinks,
  Materialization,
  MergeOn,
  WriteStrategy,
  WriteStrategyType
}
import better.files.File

trait TransformStrategiesBuilderSpec extends TestHelper {
  def engine: String
  def targetFolder = File(s"/tmp/strategies/$engine")
  new WithSettings() {
    targetFolder.delete(swallowIOExceptions = true)
    targetFolder.createDirectories()
    "create table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "CREATE"
        )
      File(targetFolder, "CREATE.sql").overwrite(finalSql)

      logger.info(
        s"create table target------------------------------------------------------------\n$finalSql"
      )
    }
    "create table strategy source and target" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND),
          on = Some(MergeOn.SOURCE_AND_TARGET),
          key = List("transaction_id")
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "CREATE"
        )
      File(targetFolder, "CREATE_SOURCE_TARGET.sql").overwrite(finalSql)

      logger.info(
        s"create table source and target------------------------------------------------------------\n$finalSql"
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
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "CREATE"
        )
      File(targetFolder, "CREATE_SCD2.sql").overwrite(finalSql)

      logger.info(
        s"create table target with scd2------------------------------------------------------------\n$finalSql"
      )
    }
    "append table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "APPEND"
        )
      File(targetFolder, "APPEND.sql").overwrite(finalSql)
      logger.info(s"append ------------------------------------------------------------\n$finalSql")
    }

    "truncate then append table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = true,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "APPEND"
        )
      File(targetFolder, "APPEND_TRUNCATE.sql").overwrite(finalSql)
      logger.info(
        s"truncate then append------------------------------------------------------------\n$finalSql"
      )
    }

    "overwrite table" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.APPEND)
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-01-01' as transaction_date, 100 as amount, 'NY' as location_info, 'John'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = true,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "OVERWRITE"
        )
      File(targetFolder, "OVERWRITE.sql").overwrite(finalSql)
      logger.info(
        s"overwrite------------------------------------------------------------\n$finalSql"
      )
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
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY"
        )
      File(targetFolder, "UPSERT_BY_KEY.sql").overwrite(finalSql)
      logger.info(
        s"upsert by key------------------------------------------------------------\n$finalSql"
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
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY"
        )
      File(targetFolder, "UPSERT_BY_KEY_SOURCE_TARGET.sql").overwrite(finalSql)
      logger.info(
        s"upsert by key source and target------------------------------------------------------------\n$finalSql"
      )
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
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY_AND_TIMESTAMP"
        )
      File(targetFolder, "UPSERT_BY_KEY_AND_TIMESTAMP.sql").overwrite(finalSql)
      logger.info(
        s"upsert by key and timestamp on target------------------------------------------------------------\n$finalSql"
      )
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
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "UPSERT_BY_KEY_AND_TIMESTAMP"
        )
      File(targetFolder, "UPSERT_BY_KEY_AND_TIMESTAMP_S_AND_T.sql").overwrite(finalSql)
      logger.info(
        s"upsert by key and timestamp on source and target------------------------------------------------------------\n$finalSql"
      )
    }

    "delete then insert" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
          key = List("transaction_id", "seller_info"),
          on = Some(MergeOn.SOURCE_AND_TARGET),
          timestamp = Some("transaction_date"),
          startTs = Some("start_ts"),
          endTs = Some("end_ts")
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "DELETE_THEN_INSERT"
        )
      File(targetFolder, "DELETE_THEN_INSERT.sql").overwrite(finalSql)
      logger.info(
        s"delete then insert------------------------------------------------------------\n$finalSql"
      )
    }

    "scd2" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.SCD2),
          key = List("transaction_id"),
          on = Some(MergeOn.TARGET),
          timestamp = Some("transaction_date"),
          startTs = Some("start_ts"),
          endTs = Some("end_ts")
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "SCD2"
        )
      File(targetFolder, "SCD2.sql").overwrite(finalSql)
      logger.info(
        s"SCD2 on source and target------------------------------------------------------------\n$finalSql"
      )
    }

    "scd2 source and target" should "return correct SQL" in {
      val strategy =
        WriteStrategy(
          `type` = Some(WriteStrategyType.SCD2),
          key = List("transaction_id"),
          on = Some(MergeOn.SOURCE_AND_TARGET),
          timestamp = Some("transaction_date"),
          startTs = Some("start_ts"),
          endTs = Some("end_ts")
        )

      val finalSql =
        new TransformStrategiesBuilder().buildSqlWithJ2(
          strategy,
          "WITH CTE AS (SELECT '12345' as transaction_id, timestamp '2021-03-01' as transaction_date, 300 as amount, 'CA' as location_info, 'Dua'  as seller_info) select * FROM CTE",
          TransformStrategiesBuilder.TableComponents(
            "",
            "BQ_TEST_DS",
            "transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info")
          ),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks(format = Some("delta")).getSink(),
          "SCD2"
        )
      File(targetFolder, "SCD2_SOURCE_AND_TARGET.sql").overwrite(finalSql)
      logger.info(
        s"SCD2 on source and target------------------------------------------------------------\n$finalSql"
      )
    }
  }
}
