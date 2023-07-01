package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.schema.model.Engine

class SqlUtilsSpec extends TestHelper {
  new WithSettings() {
    "TableRefs Extractor" should "return all tables and views" in {
      val input =
        """WITH cte1 as (query),
          |cte2 as (query2)
          |SELECT *
          |FROM myview, yourview
          |union
          |select whatever cross join herview""".stripMargin
      val refs = SQLUtils.extractRefsFromSQL(input)
      refs should contain theSameElementsAs (List("myview", "yourview", "herview"))
      // , "cte1", "cte2"))
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

    "Build Merge request" should "produce the correct sql code with update & insert statements" in {
      val sqlMerge =
        SQLUtils.buildMergeSql(
          selectWithCTEs,
          List("transaction_id"),
          Some("starlake-project-id"),
          "dataset3",
          "transactions_v3",
          Engine.BQ
        )
      sqlMerge.replaceAll("\\s", "") should be("""
          |MERGE INTO
          |`starlake-project-id`.`dataset3`.`transactions_v3` as SL_INTERNAL_SINK
          |USING(WITH
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
          |) as SL_INTERNAL_SOURCE ON SL_INTERNAL_SOURCE.transaction_id = SL_INTERNAL_SINK.transaction_id
          |WHEN MATCHED THEN UPDATE SET transaction_id = SL_INTERNAL_SOURCE.transaction_id, transaction_date = SL_INTERNAL_SOURCE.transaction_date, amount = SL_INTERNAL_SOURCE.amount, location_info = SL_INTERNAL_SOURCE.location_info, seller_info = SL_INTERNAL_SOURCE.seller_info
          |
          |WHEN NOT MATCHED THEN INSERT ("transaction_id","transaction_date","amount","location_info","seller_info") VALUES (transaction_id,transaction_date,amount,location_info,seller_info)
          |""".stripMargin.replaceAll("\\s", ""))
    }
  }
}
