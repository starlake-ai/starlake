package ai.starlake.tests

object DuckSample {
  def main(args: Array[String]): Unit = {
    Class.forName("org.duckdb.DuckDBDriver")
    import java.sql.DriverManager
    val conn = DriverManager.getConnection("jdbc:duckdb:/Users/hayssams/tmp/db/x.db")
    val stmt = conn.createStatement
    val rs = stmt.executeQuery(
      """select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE,
                                 |case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
                                 |  from INFORMATION_SCHEMA.columns
                                 |  where TABLE_SCHEMA = 'main' and TABLE_NAME = 'people'
                                 |  order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
                                 |  """.stripMargin
    )
    while (rs.next) {
      println(rs.getString(1))
    }
    conn.close()

  }
}

object SlackerDuckSample {
  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    import java.sql.DriverManager
    val conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.24:4309/x?user=main")
    val stmt = conn.createStatement
    val rs = stmt.executeQuery(
      """select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE,
        |case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
        |  from INFORMATION_SCHEMA.columns
        |  where TABLE_SCHEMA = 'main' and TABLE_NAME = 'people'
        |  order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
        |  """.stripMargin
    )

    val metaData = rs.getMetaData

    while (rs.next) {
      print(rs.getString(1) + "|")
      print(rs.getInt(2).toString + "|")
      print(rs.getString(3) + "|")
      println(rs.getString(4))
    }
    conn.close()

  }
}
