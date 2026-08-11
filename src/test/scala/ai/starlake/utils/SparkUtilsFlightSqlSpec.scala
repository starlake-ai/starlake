package ai.starlake.utils

import ai.starlake.TestHelper
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class SparkUtilsFlightSqlSpec extends TestHelper {

  private val qodUrl =
    "jdbc:arrow-flight-sql://localhost:31338?useEncryption=true" +
    "&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true"

  private val schema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType)
    )
  )

  new WithSettings() {
    "sqlSchemaString" should "generate DDL for a flight sql url" in {
      val ddl =
        SparkUtils.sqlSchemaString(schema, caseSensitive = false, qodUrl, "duckdb", Map.empty, 0)
      ddl should include("id")
      ddl should include("name")
      ddl should not include "arrow-flight-sql"
    }

    it should "look up per attribute ddl overrides with the resolved engine name" in {
      val ddl = SparkUtils.sqlSchemaString(
        schema,
        caseSensitive = false,
        qodUrl,
        "duckdb",
        Map("name" -> Map("duckdb" -> "VARCHAR(12)")),
        0
      )
      ddl should include("VARCHAR(12)")
    }

    it should "still derive the engine from the url for plain jdbc connections" in {
      val ddl = SparkUtils.sqlSchemaString(
        schema,
        caseSensitive = false,
        "jdbc:postgresql://localhost:5432/db",
        "postgresql",
        Map("name" -> Map("postgresql" -> "VARCHAR(24)")),
        0
      )
      ddl should include("VARCHAR(24)")
    }
  }
}
