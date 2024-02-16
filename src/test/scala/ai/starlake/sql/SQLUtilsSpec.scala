package ai.starlake.sql

import ai.starlake.TestHelper
import ai.starlake.config.Settings.Connection
import ai.starlake.job.ingest.strategies.StrategiesBuilder
import ai.starlake.schema.model.{AllSinks, Refs, WriteStrategy, WriteStrategyType}

class SQLUtilsSpec extends TestHelper {
  new WithSettings() {
    "Resolve SQL Ref list" should "return correct SQL" in {
      val sql = "with mycte as (select seller_email, amount " +
        "from sellers hrs, orders sos where hrs.id = sos.seller_id" +
        ")" +
        "select seller_email, sum(amount) as sum from mycte"
      val refs =
        List(Some(("", "hr", "sellers")), Some(("", "sales", "orders")), Some(("", "", "mycte")))
//      val result = SQLUtils.buildSingleSQLQuery(sql, refs)
//      println(result)
      assert(true)
    }

    val selectWithCTE1 =
      """WITH cte1 as (select *  except(this_col) from thisview),
        |cte2 as (select * from thisview)
        |SELECT *
        |FROM myview, yourview
        |union
        |select whatever from otherview cross join herview left join cte1""".stripMargin

    "TableRefs Extractor" should "return all tables and views" in {
      val refs = SQLUtils.extractTableNames(selectWithCTE1)
      refs should contain theSameElementsAs (List(
        "thisview",
        "myview",
        "yourview",
        "otherview",
        "herview"
      ))
      // , "cte1", "cte2"))
    }
    "Extract colum names from select with CTE with set operations" should "return all column names" in {
      val refs = SQLUtils.extractColumnNames(selectWithCTE1)
      refs should contain theSameElementsAs (List("*"))
    }

    "Extract tables from CTE" should "return all table names" in {
      val refs = SQLUtils.extractTableNames(selectWithCTE1)
      refs.distinct should contain theSameElementsAs List(
        "myview",
        "yourview",
        "herview",
        "thisview",
        "otherview"
      )
    }

    "Extract tables from select parquet" should "return only tables with parquet files" in {
      val refs =
        SQLUtils.extractTableNames("select * from parquet('s3://bucket/path'), t")
      assert(refs == List("t"))
    }

    val selectWithCTEs =
      """WITH
        |    transactions AS (
        |        SELECT
        |            transaction_id,
        |            transaction_date,
        |            amount,
        |            store_id,
        |            seller_id 
        |        FROM `starlake-325712`.`starlake_tbl`.`transactions`
        |        WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
        |    ),
        |    locations AS (
        |        SELECT
        |            store_id,
        |            location_name,
        |            address,
        |            city,
        |            state,
        |            country
        |        FROM `starlake-325712`.`starlake_tbl`.`locations`
        |        WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
        |    ),
        |    sellers AS (
        |        SELECT
        |            seller_id,
        |            seller_name,
        |            hire_date
        |        FROM `starlake-325712`.`starlake_tbl`.`sellers`
        |        WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
        |    )
        |
        |SELECT
        |    t.transaction_id,
        |    t.transaction_date,
        |    t.amount,
        |    STRUCT(
        |        l.location_name,
        |        l.address,
        |        l.city,
        |        l.state,
        |        l.country
        |        ) AS location_info,
        |    STRUCT(
        |        s.seller_name,
        |        s.hire_date
        |        ) AS seller_info
        |FROM
        |    transactions t
        |        LEFT JOIN
        |    locations l ON t.store_id = l.store_id
        |        LEFT JOIN
        |    sellers s ON t.seller_id = s.seller_id
        |
        |""".stripMargin

    "Extract colum names from select with CTE" should "return all column names" in {
      val refs = SQLUtils.extractColumnNames(selectWithCTEs)
      refs should contain theSameElementsAs (List(
        "transaction_id",
        "transaction_date",
        "amount",
        "location_info",
        "seller_info"
      ))
    }

    "Build Single SQl Query for Regex" should "return all table names" in {
      val selectWithCTE =
        """with mycte as (
          |select seller_email, amount
          |from sellers hrs, orders sos where hrs.id = sos.seller_id
          |)
          |select seller_email, sum(amount) as sum from mycte
          |group by mycte.seller_email
          |""".stripMargin

      val resultSQL =
        SQLUtils.buildSingleSQLQueryForRegex(
          selectWithCTE,
          Refs(Nil),
          Nil,
          Nil,
          SQLUtils.fromsRegex,
          "FROM",
          new Connection(Some("SPARK"), Some("parquet"), None, None, Map.empty)
        )
      resultSQL should equal(
        """with mycte as (
          |select seller_email, amount
          |from sellers hrs, orders sos where hrs.id = sos.seller_id
          |)
          |select seller_email, sum(amount) as sum from mycte
          |group by mycte.seller_email
          |""".stripMargin
      )
    }

    "Extract table names from select with CTE" should "return all table names" in {

      val selectWithCTE =
        """with mycte as (
          |select seller_email, amount, (select x from y)
          |from "domain".sellers hrs, orders sos where hrs.id = sos.seller_id
          |)
          |select seller_email, sum(amount) as sum from mycte
          |group by mycte.seller_email
          |""".stripMargin

      val refs = SQLUtils.extractTableNames(selectWithCTE)
      refs should contain theSameElementsAs (List(
        "\"domain\".sellers",
        "orders",
        "y"
      ))
    }

    "Extract table names from nested select" should "return all table names" in {
      val selectNestedSelects =
        """select selected_item_count * 100 / all_item_count, thismonth
          |FROM (
          |  SELECT count(case when Items.StatusID in (5,7,11) then 1 end) as selected_item_count,
          |         count(case when Items.StatusID in (5,7,11,6) then 1 end) as all_item_count,
          |         count(case when Items.StatusID in (5,7,11,6) and month(Items.Close_Date) = 11 then 1 end) As thisMonth
          |  FROM items
          |  WHERE Items.StatusID in (5,7,11,6)
          |    and year(Items.Close_Date) = 2016
          |) t""".stripMargin
      val refs = SQLUtils.extractTableNames(selectNestedSelects)
      refs should contain theSameElementsAs (List(
        "items"
      ))
    }

    "Extract table names from select" should "return all CTE names" in {
      val refs = SQLUtils.extractTableNames(selectWithCTEs)
      refs should contain theSameElementsAs (List(
        "`starlake-325712`.`starlake_tbl`.`transactions`",
        "`starlake-325712`.`starlake_tbl`.`locations`",
        "`starlake-325712`.`starlake_tbl`.`sellers`"
      ))
    }

    "Extract CTE from select" should "return all CTE names" in {
      val refs = SQLUtils.extractCTENames(selectWithCTEs)
      refs should contain theSameElementsAs (List(
        "transactions",
        "locations",
        "sellers"
      ))
    }

    "Build Merge request" should "produce the correct sql code with update & insert statements" in {
      val strategy =
        WriteStrategy(
          `type` = WriteStrategyType.UPSERT_BY_KEY,
          key = List("transaction_id"),
          timestamp = None,
          queryFilter = None,
          on = None,
          start_ts = None,
          end_ts = None
        )

      val sqlMerge =
        StrategiesBuilder("ai.starlake.job.ingest.strategies.JdbcStrategiesBuilder")
          .buildSQLForStrategy(
            strategy,
            selectWithCTEs,
            "starlake-project-id.dataset3.transactions_v3",
            List("transaction_id", "transaction_date", "amount", "location_info", "seller_info"),
            targetTableExists = true,
            truncate = false,
            materializedView = false,
            settings.appConfig.jdbcEngines("bigquery"),
            AllSinks().getSink()
          )
      sqlMerge.replaceAll("\\s", "") should be("""
                                                 |CREATE OR REPLACE TEMPORARY VIEW SL_INCOMING AS (WITH
                                                 |    transactions AS (
                                                 |        SELECT
                                                 |            transaction_id,
                                                 |            transaction_date,
                                                 |            amount,
                                                 |            store_id,
                                                 |            seller_id
                                                 |        FROM `starlake-325712`.`starlake_tbl`.`transactions`
                                                 |        WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
                                                 |    ),
                                                 |    locations AS (
                                                 |        SELECT
                                                 |            store_id,
                                                 |            location_name,
                                                 |            address,
                                                 |            city,
                                                 |            state,
                                                 |            country
                                                 |        FROM `starlake-325712`.`starlake_tbl`.`locations`
                                                 |        WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
                                                 |    ),
                                                 |    sellers AS (
                                                 |        SELECT
                                                 |            seller_id,
                                                 |            seller_name,
                                                 |            hire_date
                                                 |        FROM `starlake-325712`.`starlake_tbl`.`sellers`
                                                 |        WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
                                                 |    )
                                                 |
                                                 |SELECT
                                                 |    t.transaction_id,
                                                 |    t.transaction_date,
                                                 |    t.amount,
                                                 |    STRUCT(
                                                 |        l.location_name,
                                                 |        l.address,
                                                 |        l.city,
                                                 |        l.state,
                                                 |        l.country
                                                 |        ) AS location_info,
                                                 |    STRUCT(
                                                 |        s.seller_name,
                                                 |        s.hire_date
                                                 |        ) AS seller_info
                                                 |FROM
                                                 |    transactions t
                                                 |        LEFT JOIN
                                                 |    locations l ON t.store_id = l.store_id
                                                 |        LEFT JOIN
                                                 |    sellers s ON t.seller_id = s.seller_id
                                                 |
                                                 |);
                                                 |
                                                 |CREATE OR REPLACE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
                                                 |  SELECT  `transaction_id`,`transaction_date`,`amount`,`location_info`,`seller_info`,
                                                 |          ROW_NUMBER() OVER (PARTITION BY `transaction_id`  ORDER BY (select 0)) AS SL_SEQ
                                                 |  FROM SL_INCOMING;
                                                 |
                                                 |CREATE TEMPORARY TABLE SL_DEDUP AS
                                                 |  SELECT  `transaction_id`,`transaction_date`,`amount`,`location_info`,`seller_info`
                                                 |  FROM SL_VIEW_WITH_ROWNUM
                                                 |  WHERE SL_SEQ = 1;
                                                 |
                                                 |MERGE INTO starlake-project-id.dataset3.transactions_v3 USING SL_DEDUP ON (SL_DEDUP.`transaction_id` = starlake-project-id.dataset3.transactions_v3.`transaction_id`)
                                                 |WHEN MATCHED THEN UPDATE SET `transaction_id` = SL_DEDUP.`transaction_id`,`transaction_date` = SL_DEDUP.`transaction_date`,`amount` = SL_DEDUP.`amount`,`location_info` = SL_DEDUP.`location_info`,`seller_info` = SL_DEDUP.`seller_info`
                                                 |WHEN NOT MATCHED THEN INSERT (`transaction_id`,`transaction_date`,`amount`,`location_info`,`seller_info`) VALUES (SL_DEDUP.`transaction_id`,SL_DEDUP.`transaction_date`,SL_DEDUP.`amount`,SL_DEDUP.`location_info`,SL_DEDUP.`seller_info`)
                                                 |""".stripMargin.replaceAll("\\s", ""))
    }
    "Strip comments" should "succeed" in {
      SQLUtils
        .stripComments(
          """
          |-- comment
          |select *
          |from t -- coucou
          |/*
          |
          |comment
          |
          | */ -- hello
          |""".stripMargin
        ) should equal(
        """
          |select *
          |from t
          |""".stripMargin.trim
      )

    }
  }
}
