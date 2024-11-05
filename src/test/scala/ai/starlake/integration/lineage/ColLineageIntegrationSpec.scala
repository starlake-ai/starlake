package ai.starlake.integration.lineage

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.JdbcMetaData
import better.files.File

class ColLineageIntegrationSpec extends IntegrationTestBase {

  override def templates: File = starlakeDir / "samples"
  override def localDir: File = templates / "spark"
  override def sampleDataDir: File = localDir / "sample-data"

  "Lineage Generation1" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      assert(
        new Main().run(
          Array("col-lineage", "--task", "sales_kpi.byseller_kpi0")
        )
      )
    }
  }

  "Lineage Generation2" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      assert(
        new Main().run(
          Array("col-lineage", "--task", "sales_kpi.byseller_kpi1")
        )
      )
    }
  }

  "Lineage" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      new Main().run(
        Array("col-lineage", "--task", "sales_kpi.byseller_kpi0")
      )
    }
  }
  "Lineage with multiple input cols" should "succeed" in {
    withEnvs("SL_ROOT" -> (localDir.parent / "starbake").pathAsString) {
      new Main().run(
        Array("col-lineage", "--task", "kpi.order_summary")
      )
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
