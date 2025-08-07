package ai.starlake.integration.lineage

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.diff.DBSchema
import ai.starlake.transpiler.schema.JdbcMetaData
import better.files.File

import java.util
import scala.jdk.CollectionConverters.*

class ColLineageIntegrationSpec extends IntegrationTestBase {
  "Lineage Generation1" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString) {
      assert(
        new Main().run(
          Array("col-lineage", "--task", "sales_kpi.byseller_kpi0")
        )
      )
    }
  }

  "Lineage Generation2" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString) {
      assert(
        new Main().run(
          Array("col-lineage", "--task", "sales_kpi.byseller_kpi1")
        )
      )
    }
  }

  "Lineage" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString) {
      new Main().run(
        Array("col-lineage", "--task", "sales_kpi.byseller_kpi0")
      )
    }
  }

  "Lineage with multiple input cols" should "succeed" in {
    withEnvs("SL_ROOT" -> (theSampleFolder.parent / "lineage").pathAsString) {
      val tmpFile = File.newTemporaryFile()
      new Main().run(
        Array(
          "col-lineage",
          "--task",
          "starbake_analytics.order_items_analysis",
          "--output",
          tmpFile.pathAsString
        )
      )
      val expected = """{
                       |  "tables" : [ {
                       |    "domain" : "starbake",
                       |    "table" : "products",
                       |    "columns" : [ "name", "price", "category", "cost", "description", "product_id" ],
                       |    "isTask" : false
                       |  }, {
                       |    "domain" : "starbake_analytics",
                       |    "table" : "order_items_analysis",
                       |    "columns" : [ "order_id", "order_date", "customer_id", "purchased_items", "total_order_value" ],
                       |    "isTask" : true
                       |  }, {
                       |    "domain" : "starbake",
                       |    "table" : "orders",
                       |    "columns" : [ "order_id", "order_date", "customer_id", "quantity", "product_id" ],
                       |    "isTask" : false
                       |  }, {
                       |    "table" : "order_details",
                       |    "columns" : [ "order_id", "order_date", "customer_id", "purchased_items", "total_order_value" ],
                       |    "isTask" : false
                       |  } ],
                       |  "relations" : [ {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "order_id"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "order_id"
                       |    },
                       |    "expression" : "order_id"
                       |  }, {
                       |    "from" : {
                       |      "table" : "order_details",
                       |      "column" : "order_id"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "order_items_analysis",
                       |      "column" : "order_id"
                       |    },
                       |    "expression" : "order_id"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "order_date"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "order_date"
                       |    },
                       |    "expression" : "order_date"
                       |  }, {
                       |    "from" : {
                       |      "table" : "order_details",
                       |      "column" : "order_date"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "order_items_analysis",
                       |      "column" : "order_date"
                       |    },
                       |    "expression" : "order_date"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "customer_id"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "customer_id"
                       |    },
                       |    "expression" : "customer_id"
                       |  }, {
                       |    "from" : {
                       |      "table" : "order_details",
                       |      "column" : "customer_id"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "order_items_analysis",
                       |      "column" : "customer_id"
                       |    },
                       |    "expression" : "customer_id"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "products",
                       |      "column" : "name"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "purchased_items"
                       |    },
                       |    "expression" : "List(p.name || ' (' || o.quantity || ')')"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "quantity"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "purchased_items"
                       |    },
                       |    "expression" : "List(p.name || ' (' || o.quantity || ')')"
                       |  }, {
                       |    "from" : {
                       |      "table" : "order_details",
                       |      "column" : "purchased_items"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "purchased_items"
                       |    },
                       |    "expression" : "List(p.name || ' (' || o.quantity || ')')"
                       |  }, {
                       |    "from" : {
                       |      "table" : "order_details",
                       |      "column" : "purchased_items"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "order_items_analysis",
                       |      "column" : "purchased_items"
                       |    }
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "quantity"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "total_order_value"
                       |    },
                       |    "expression" : "Sum(o.quantity * p.price)"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "products",
                       |      "column" : "price"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "total_order_value"
                       |    },
                       |    "expression" : "Sum(o.quantity * p.price)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "order_details",
                       |      "column" : "total_order_value"
                       |    },
                       |    "to" : {
                       |      "table" : "order_details",
                       |      "column" : "total_order_value"
                       |    },
                       |    "expression" : "Sum(o.quantity * p.price)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "order_details",
                       |      "column" : "total_order_value"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "order_items_analysis",
                       |      "column" : "total_order_value"
                       |    }
                       |  } ]
                       |}
                       |""".stripMargin
      val res = tmpFile.contentAsString
      tmpFile.delete(swallowIOExceptions = true)
      println(res)
      assert(res.replaceAll("\\s", "") == expected.replaceAll("\\s", ""))
    }
  }

  def getStarlakeSchemas(): util.Collection[DBSchema] = {

    val schema1 = new DBSchema("", "starbake", "orders")
    val schema2 = new DBSchema("", "starbake", "products")
    val schema3 = new DBSchema(
      "",
      "starbake",
      "customers"
    )
    List(schema1, schema2, schema3).asJavaCollection
  }

  "Lineage with functions" should "succeed" in {
    val sqlStr =
      """
        |WITH customer_orders AS (
        |    SELECT
        |        o.customer_id,
        |        COUNT(DISTINCT o.order_id) AS total_orders,
        |        SUM(o.quantity * p.price) AS total_spent,
        |        MIN(o.order_date) AS first_order_date,
        |        MAX(o.order_date) AS last_order_date,
        |        ARRAY_AGG(DISTINCT p.category) AS purchased_categories
        |--        LIST(DISTINCT p.category) AS purchased_categories
        |    FROM
        |        starbake.orders o
        |            JOIN
        |        starbake.products p ON o.product_id = p.product_id
        |    GROUP BY
        |        o.customer_id
        |)
        |SELECT
        |    co.customer_id,
        |    concat(c.first_name,' ', c.last_name) AS customer_name,
        |    c.email,
        |    co.total_orders,
        |    co.total_spent,
        |    co.first_order_date,
        |    co.last_order_date,
        |    co.purchased_categories,
        |    (CAST(co.last_order_date as DATE) - CAST(co.first_order_date as DATE)) as days_since_first_order
        |--    DATEDIFF('day', co.first_order_date, co.last_order_date) AS days_since_first_order
        |FROM
        |    starbake.customers c
        |        LEFT JOIN
        |    customer_orders co ON c.id = co.customer_id
        |ORDER BY
        |    co.total_spent DESC NULLS LAST;
        |
        |""".stripMargin
    val meta = new JdbcMetaData(getStarlakeSchemas())
    val res = JSQLColumResolver.getResultSetMetaData(
      sqlStr,
      meta.setErrorMode(JdbcMetaData.ErrorMode.LENIENT)
    )
    val tbl = res.getScopeTable(3)
    assert("c" == tbl)
  }
  "Lineage with multiple input cols2" should "succeed" in {
    withEnvs("SL_ROOT" -> (theSampleFolder.parent / "lineage").pathAsString) {
      val tmpFile = File.newTemporaryFile()
      new Main().run(
        Array(
          "col-lineage",
          "--task",
          "starbake_analytics.customer_purchase_history",
          "--output",
          tmpFile.pathAsString
        )
      )
      val expected = """{
                       |  "tables" : [ {
                       |    "domain" : "starbake_analytics",
                       |    "table" : "customer_purchase_history",
                       |    "columns" : [ "customer_id", "customer_name", "email", "total_orders", "total_spent", "first_order_date", "last_order_date", "purchased_categories", "days_since_first_order" ],
                       |    "isTask" : true
                       |  }, {
                       |    "domain" : "starbake",
                       |    "table" : "products",
                       |    "columns" : [ "price", "category", "cost", "description", "name", "product_id" ],
                       |    "isTask" : false
                       |  }, {
                       |    "domain" : "starbake",
                       |    "table" : "customers",
                       |    "columns" : [ "first_name", "last_name", "email", "id", "join_date" ],
                       |    "isTask" : false
                       |  }, {
                       |    "table" : "customer_orders",
                       |    "columns" : [ "customer_id", "total_orders", "total_spent", "first_order_date", "last_order_date", "purchased_categories" ],
                       |    "isTask" : false
                       |  }, {
                       |    "domain" : "starbake",
                       |    "table" : "orders",
                       |    "columns" : [ "customer_id", "order_id", "quantity", "order_date", "product_id" ],
                       |    "isTask" : false
                       |  } ],
                       |  "relations" : [ {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "customer_id"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "customer_id"
                       |    },
                       |    "expression" : "customer_id"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "customer_id"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "customer_id"
                       |    },
                       |    "expression" : "customer_id"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "customers",
                       |      "column" : "first_name"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "customer_name"
                       |    },
                       |    "expression" : "concat(c.first_name, ' ', c.last_name)"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "customers",
                       |      "column" : "last_name"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "customer_name"
                       |    },
                       |    "expression" : "concat(c.first_name, ' ', c.last_name)"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "customers",
                       |      "column" : "email"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "email"
                       |    },
                       |    "expression" : "email"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "order_id"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_orders"
                       |    },
                       |    "expression" : "COUNT(DISTINCT o.order_id)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_orders"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_orders"
                       |    },
                       |    "expression" : "COUNT(DISTINCT o.order_id)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_orders"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "total_orders"
                       |    }
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "quantity"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_spent"
                       |    },
                       |    "expression" : "SUM(o.quantity * p.price)"
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "products",
                       |      "column" : "price"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_spent"
                       |    },
                       |    "expression" : "SUM(o.quantity * p.price)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_spent"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_spent"
                       |    },
                       |    "expression" : "SUM(o.quantity * p.price)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "total_spent"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "total_spent"
                       |    }
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "order_date"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "first_order_date"
                       |    },
                       |    "expression" : "MIN(o.order_date)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "first_order_date"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "first_order_date"
                       |    },
                       |    "expression" : "MIN(o.order_date)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "first_order_date"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "first_order_date"
                       |    }
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "orders",
                       |      "column" : "order_date"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "last_order_date"
                       |    },
                       |    "expression" : "MAX(o.order_date)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "last_order_date"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "last_order_date"
                       |    },
                       |    "expression" : "MAX(o.order_date)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "last_order_date"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "last_order_date"
                       |    }
                       |  }, {
                       |    "from" : {
                       |      "domain" : "starbake",
                       |      "table" : "products",
                       |      "column" : "category"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "purchased_categories"
                       |    },
                       |    "expression" : "array_agg(DISTINCT p.category)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "purchased_categories"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "purchased_categories"
                       |    },
                       |    "expression" : "array_agg(DISTINCT p.category)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "purchased_categories"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "purchased_categories"
                       |    }
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "first_order_date"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "days_since_first_order"
                       |    },
                       |    "expression" : "DATEDIFF('day', co.first_order_date, co.last_order_date)"
                       |  }, {
                       |    "from" : {
                       |      "table" : "customer_orders",
                       |      "column" : "last_order_date"
                       |    },
                       |    "to" : {
                       |      "domain" : "starbake_analytics",
                       |      "table" : "customer_purchase_history",
                       |      "column" : "days_since_first_order"
                       |    },
                       |    "expression" : "DATEDIFF('day', co.first_order_date, co.last_order_date)"
                       |  } ]
                       |}
                       |""".stripMargin
      val res = tmpFile.contentAsString
      tmpFile.delete(swallowIOExceptions = true)
      println(res)
      assert(res.replaceAll("\\s", "") == expected.replaceAll("\\s", ""))
    }
  }

  "raw lineage" should "succeed" in {
    val sqlStr =
      "SELECT " +
      "Case when Sum(colBA + colBB) = 0 then " +
      "c.col1 else a.col2 " +
      "end AS total " +
      "FROM a " +
      "INNER JOIN " +
      "(SELECT * FROM b) c " +
      "ON a.col1 = c.col1"
    val expected = Array[Array[String]](Array("", "CaseExpression", "total"))
    val schemaDefinition = Array(
      // Table A with Columns col1, col2, col3, colAA, colAB
      Array(
        "a",
        "col1",
        "col2",
        "col3",
        "colAA",
        "colAB"
      ), // Table B with Columns col1, col2, col3, colBA, colBB
      Array("b", "col1", "col2", "col3", "colBA", "colBB")
    )

    val resolver = new JSQLColumResolver(schemaDefinition)

    val res = resolver.getResultSetMetaData(sqlStr)
    println(res)
    //@formatter:off
    val lineage = "SELECT\n" +
      " └─total AS CaseExpression: CASE WHEN Sum(colBA + colBB) = 0 THEN c.col1 ELSE a.col2 END\n" +
      "    ├─WhenClause: WHEN Sum(colBA + colBB) = 0 THEN c.col1\n" +
      "    │  ├─EqualsTo: Sum(colBA + colBB) = 0\n" +
      "    │  │  ├─Function Sum\n" +
      "    │  │  │  └─Addition: colBA + colBB\n" +
      "    │  │  │     ├─c.colBA → b.colBA : Other\n" +
      "    │  │  │     └─c.colBB → b.colBB : Other\n" +
      "    │  │  └─LongValue: 0\n" +
      "    │  └─c.col1 → b.col1 : Other\n" +
      "    └─a.col2 : Other";
    //@formatter:on
  }
  "raw lineage lenient" should "succeed" in {
    val sqlStr =
      "SELECT " +
      "Case when Sum(colBA + colBB)=0 then " +
      "c.col1 else a.col2 " +
      "end AS total " +
      "FROM a " +
      "INNER JOIN " +
      "(SELECT * FROM b) c " +
      "ON a.col1 = c.col1"
    val expected = Array[Array[String]](Array("", "CaseExpression", "total"))
    val schemaDefinition = Array(
      // Table B with Columns col1, col2, col3, colBA, colBB
      Array("b", "col1", "col2", "col3", "colBA", "colBB")
    )

    val metaData = new JdbcMetaData("", "", schemaDefinition)
    val res =
      JSQLColumResolver.getResultSetMetaData(
        sqlStr,
        JdbcMetaData.copyOf(metaData.setErrorMode(JdbcMetaData.ErrorMode.LENIENT))
      )
    println(res)
    //@formatter:off
    val lineage = "SELECT\n" +
      " └─total AS CaseExpression: CASE WHEN Sum(colBA + colBB) = 0 THEN c.col1 ELSE a.col2 END\n" +
      "    ├─WhenClause: WHEN Sum(colBA + colBB) = 0 THEN c.col1\n" +
      "    │  ├─EqualsTo: Sum(colBA + colBB) = 0\n" +
      "    │  │  ├─Function Sum\n" +
      "    │  │  │  └─Addition: colBA + colBB\n" +
      "    │  │  │     ├─c.colBA → b.colBA : Other\n" +
      "    │  │  │     └─c.colBB → b.colBB : Other\n" +
      "    │  │  └─LongValue: 0\n" +
      "    │  └─c.col1 → b.col1 : Other\n" +
      "    └─a.col2 : Other";
    //@formatter:on
  }

  "raw lineage2" should "succeed" in {
    val sqlStr =
      "SELECT Sum(colBA + colBB) AS total FROM a INNER JOIN (SELECT * FROM b) c ON a.col1 = c.col1"

    val schemaDefinition = Array(
      // Table A with Columns col1, col2, col3, colAA, colAB
      Array(
        "a",
        "col1",
        "col2",
        "col3",
        "colAA",
        "colAB"
      ), // Table B with Columns col1, col2, col3, colBA, colBB
      Array("b", "col1", "col2", "col3", "colBA", "colBB")
    )

    val resolver = new JSQLColumResolver(schemaDefinition)

    val res = resolver.getResultSetMetaData(sqlStr)
    println(res)
    //@formatter:off
    val lineage =
      "SELECT\n" +
        " └─total AS Function Sum\n" +
        "    └─Addition: colBA + colBB\n" +
        "       ├─c.colBA → b.colBA : Other\n" +
        "       └─c.colBB → b.colBB : Other"
    ;

    //@formatter:on
  }
}
