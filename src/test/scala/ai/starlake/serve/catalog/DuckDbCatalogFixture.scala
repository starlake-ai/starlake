package ai.starlake.serve.catalog

import ai.starlake.config.ConnectionInfo
import ai.starlake.schema.model.ConnectionType

import java.nio.file.Files
import java.sql.DriverManager
import scala.util.Using

object DuckDbCatalogFixture {
  val BrokenPassword = "hunter2-secret"

  /** Creates a DuckDB file with:
    *   - schema `sales`: table `orders` (id INTEGER NOT NULL, amount DECIMAL(10,2), created DATE,
    *     note VARCHAR), view `big_orders`, table `we'ird."name"` (id INTEGER), 3 rows in orders
    *   - schema `s_les`: table `other` (x INTEGER), whose name is a LIKE pattern matching `sales`
    */
  def create(): (String, Map[String, ConnectionInfo]) = {
    val dir = Files.createTempDirectory("catalog-spec")
    val dbPath = dir.resolve("catalog.duckdb").toString
    Class.forName("org.duckdb.DuckDBDriver")
    Using.resource(DriverManager.getConnection(s"jdbc:duckdb:$dbPath")) { conn =>
      Using.resource(conn.createStatement()) { st =>
        List(
          "CREATE SCHEMA sales",
          "CREATE SCHEMA s_les",
          """CREATE TABLE sales.orders (
            |  id INTEGER NOT NULL,
            |  amount DECIMAL(10,2),
            |  created DATE,
            |  note VARCHAR
            |)""".stripMargin,
          "COMMENT ON TABLE sales.orders IS 'All orders'",
          """INSERT INTO sales.orders VALUES
            |  (1, 12.50, DATE '2024-01-31', 'first'),
            |  (2, 99.99, DATE '2024-02-01', NULL),
            |  (3, 0.10, NULL, 'third')""".stripMargin,
          "CREATE VIEW sales.big_orders AS SELECT * FROM sales.orders WHERE amount > 50",
          "CREATE TABLE sales.\"we'ird.\"\"name\"\"\" (id INTEGER)",
          "INSERT INTO sales.\"we'ird.\"\"name\"\"\" VALUES (42)",
          "CREATE TABLE s_les.other (x INTEGER)"
        ).foreach(st.execute)
      }
    }
    val connections = Map(
      "duck" -> ConnectionInfo(
        ConnectionType.JDBC,
        options = Map("url" -> s"jdbc:duckdb:$dbPath", "driver" -> "org.duckdb.DuckDBDriver")
      ),
      "lake" -> ConnectionInfo(ConnectionType.FS, options = Map.empty),
      "broken" -> ConnectionInfo(
        ConnectionType.JDBC,
        options = Map(
          "url"      -> "jdbc:postgresql://127.0.0.1:1/nodb",
          "driver"   -> "org.postgresql.Driver",
          "user"     -> "bob",
          "password" -> BrokenPassword
        )
      )
    )
    (dbPath, connections)
  }
}
