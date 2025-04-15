package ai.starlake.integration.lineage

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.JdbcMetaData
import better.files.File

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
                       |    "columns" : [ "order_id", "order_date", "customer_id" ],
                       |    "isTask" : false
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
                       |  } ]
                       |}""".stripMargin
      val res = tmpFile.contentAsString
      tmpFile.delete(swallowIOExceptions = true)
      println(res)
      assert(res.replaceAll("\\s", "") == expected.replaceAll("\\s", ""))
    }
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
                       |    "columns" : [ "customer_id", "customer_name", "email" ],
                       |    "isTask" : false
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
                       |      "domain" : "starbake",
                       |      "table" : "products",
                       |      "column" : "category"
                       |    },
                       |    "to" : {
                       |      "table" : "customer_orders",
                       |      "column" : "purchased_categories"
                       |    },
                       |    "expression" : "LIST(DISTINCT p.category)"
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
                       |    "expression" : "DATEDIFF('day', co.first_order_date, co.last_order_date)"
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
