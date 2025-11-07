package ai.starlake.sql

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.types.*

object SQLTypeMappings extends StrictLogging {
  val bigqueryToSparkMap: Map[String, String] = Map(
    // Numeric Types
    "INTEGER"    -> LongType,
    "INT64"      -> LongType,
    "NUMERIC"    -> DecimalType(38, 9), // BigQuery NUMERIC has 38 total digits, 9 scale
    "BIGNUMERIC" -> DecimalType(38, 38), // BigQuery BIGNUMERIC
    "FLOAT64"    -> DoubleType,

    // Boolean Type
    "BOOL" -> BooleanType,

    // String Types
    "STRING" -> StringType,

    // Byte Type
    "BYTES" -> BinaryType,

    // Date/Time Types
    "DATE"      -> DateType,
    "DATETIME"  -> TimestampType, // Spark uses TimestampType for both BQ DATETIME and TIMESTAMP
    "TIMESTAMP" -> TimestampType,
    "TIME"      -> StringType, // Spark doesn't have a distinct TIME type; often mapped to String

    // Complex/Semi-Structured Types
    "STRUCT" -> StructType(Seq()), // Placeholder, schema must be defined dynamically
    "RECORD" -> StructType(Seq()), // Alias for STRUCT
    "ARRAY"  -> ArrayType(NullType), // Placeholder, element type must be defined dynamically
    "JSON"   -> StringType, // BQ JSON is often read as String in Spark

    // Geospatial Type
    "GEOGRAPHY" -> StringType // BQ GEOGRAPHY (WKT or GeoJson string) maps to String
  ).map(kv => kv._1.toLowerCase -> kv._2.typeName) // Normalize keys to lowercase

  val snowflakeToSparkMap: Map[String, String] = Map(
    // Fixed-Point Numeric Types
    "NUMBER" -> DecimalType(38, 0), // Default mapping for NUMBER (max precision/scale)
    "DECIMAL" -> DecimalType(
      38,
      18
    ), // Example precision/scale, often configured based on source table DDL
    "NUMERIC"  -> DecimalType(38, 18),
    "INT"      -> LongType,
    "INTEGER"  -> LongType,
    "BIGINT"   -> LongType,
    "SMALLINT" -> ShortType,
    "TINYINT"  -> ByteType,

    // Floating-Point Numeric Types
    "FLOAT"  -> DoubleType,
    "FLOAT4" -> FloatType,
    "FLOAT8" -> DoubleType,
    "DOUBLE" -> DoubleType,

    // String and Binary Types
    "VARCHAR"   -> StringType,
    "CHAR"      -> StringType,
    "STRING"    -> StringType,
    "TEXT"      -> StringType,
    "BINARY"    -> BinaryType,
    "VARBINARY" -> BinaryType,

    // Boolean Type
    "BOOLEAN" -> BooleanType,

    // Date and Time Types
    "DATE" -> DateType,
    // Snowflake has 3 TIMESTAMP variants. All typically map to Spark's TimestampType.
    "TIMESTAMP_NTZ" -> TimestampType, // No Time Zone (NTZ)
    "TIMESTAMP_LTZ" -> TimestampType, // Local Time Zone (LTZ)
    "TIMESTAMP_TZ"  -> TimestampType, // Time Zone (TZ)
    "TIME" -> StringType, // Spark lacks a distinct TIME type; often handled as String or Timestamp conversion

    // Semi-Structured Types (often read as String/Binary, but native format is preferred)
    "VARIANT" -> StringType, // Most common default; can also be StringType
    "OBJECT"  -> StringType,
    "ARRAY"   -> ArrayType(NullType) // Placeholder; element type defined by source schema
  ).map(kv => kv._1.toLowerCase -> kv._2.typeName) // Normalize keys to lowercase

  val duckDBToSparkMap: Map[String, String] = Map(
    // Boolean Type
    "BOOLEAN" -> BooleanType,

    // Integer Types
    "TINYINT"  -> ByteType,
    "SMALLINT" -> ShortType,
    "INTEGER"  -> IntegerType,
    "INT"      -> IntegerType, // Alias
    "BIGINT"   -> LongType,
    "HUGEINT" -> LongType, // DecimalType(38, 0), // DuckDB's 128-bit integer maps safely to max Decimal

    // Floating Point Types
    "REAL"   -> FloatType, // 4-byte float
    "FLOAT"  -> FloatType, // Alias
    "DOUBLE" -> DoubleType, // 8-byte float

    // Decimal/Numeric Types
    // DuckDB supports configurable precision/scale (DECIMAL(P, S)).
    // We use a high-precision default for generic mapping.
    "DECIMAL" -> DecimalType(38, 18),
    "NUMERIC" -> DecimalType(38, 18), // Alias

    // String and Binary Types
    "VARCHAR" -> StringType,
    "CHAR"    -> StringType,
    "BPCHAR"  -> StringType, // Blank-padded character (fixed length)
    "TEXT"    -> StringType,
    "BLOB"    -> BinaryType,
    "BYTEA"   -> BinaryType, // Alias for BLOB

    // Date/Time Types
    "DATE"                     -> DateType,
    "TIME"                     -> StringType, // Spark lacks a distinct TIME type; mapped to String
    "TIMESTAMP"                -> TimestampType, // Represents TIMESTAMP_S/MS/US/NS/TZ
    "TIMESTAMP WITH TIME ZONE" -> TimestampType, // Spark handles TZ internally

    // UUID Type
    "UUID" -> StringType, // UUIDs are typically represented as strings in Spark

    // Complex Types
    "STRUCT" -> StructType(Seq()), // Placeholder, nested schema required
    "LIST"   -> ArrayType(NullType), // DuckDB's LIST maps to Spark's ArrayType
    "MAP"    -> MapType(StringType, NullType) // DuckDB's MAP maps to Spark's MapType
  ).map(kv => kv._1.toLowerCase -> kv._2.typeName) // Normalize keys to lowercase

  val postgreSqlToSparkMap: Map[String, String] = Map(
    // Boolean Type
    "BOOLEAN" -> BooleanType,

    // Integer Types
    "SMALLINT" -> ShortType, // 2 bytes
    "INT2"     -> ShortType, // Alias for SMALLINT
    "INTEGER"  -> IntegerType, // 4 bytes
    "INT4"     -> IntegerType, // Alias for INTEGER
    "BIGINT"   -> LongType, // 8 bytes
    "INT8"     -> LongType, // Alias for BIGINT

    // Floating Point Types
    "REAL"             -> FloatType, // 4-byte single precision
    "FLOAT4"           -> FloatType, // Alias
    "DOUBLE PRECISION" -> DoubleType, // 8-byte double precision
    "FLOAT8"           -> DoubleType, // Alias

    // Decimal/Numeric Types
    // Uses a high-precision default, actual precision/scale should match column DDL
    "NUMERIC" -> DecimalType(38, 18),
    "DECIMAL" -> DecimalType(38, 18),

    // String and Binary Types
    "VARCHAR" -> StringType,
    "CHAR"    -> StringType,
    "TEXT"    -> StringType,
    "BYTEA"   -> BinaryType, // Binary string

    // Date/Time Types
    "DATE"   -> DateType,
    "TIME"   -> StringType, // TIME WITHOUT TIME ZONE; Mapped to String
    "TIMETZ" -> StringType, // TIME WITH TIME ZONE; Mapped to String
    // Spark's TimestampType handles both TZ and non-TZ timestamps
    "TIMESTAMP"   -> TimestampType, // TIMESTAMP WITHOUT TIME ZONE
    "TIMESTAMPTZ" -> TimestampType, // TIMESTAMP WITH TIME ZONE

    // JSON/JSONB Types
    "JSON"  -> StringType,
    "JSONB" -> StringType,

    // UUID Type
    "UUID" -> StringType,

    // Array Types
    // This is a placeholder for PostgreSQL's native array types (e.g., INT4[])
    "_int4" -> ArrayType(IntegerType),
    "_text" -> ArrayType(StringType),
    // Generic mapping for any other array, actual element type must be determined
    "ARRAY" -> ArrayType(NullType)
  ).map(kv => kv._1.toLowerCase -> kv._2.typeName) // Normalize keys to lowercase

  val redshiftToSparkMap: Map[String, String] = Map(
    // Numeric Types
    "SMALLINT" -> ShortType,
    "INT2"     -> ShortType,
    "INTEGER"  -> IntegerType,
    "INT"      -> IntegerType,
    "INT4"     -> IntegerType,
    "BIGINT"   -> LongType,
    "INT8"     -> LongType,

    // Decimal Types
    // Redshift NUMERIC/DECIMAL supports up to 38 digits precision.
    // We use a high-precision default for generic mapping.
    "NUMERIC" -> DecimalType(38, 18),
    "DECIMAL" -> DecimalType(38, 18),

    // Floating Point Types
    "REAL"             -> FloatType, // 4-byte single precision
    "FLOAT4"           -> FloatType,
    "DOUBLE PRECISION" -> DoubleType, // 8-byte double precision
    "FLOAT"            -> DoubleType,
    "FLOAT8"           -> DoubleType,

    // String and Binary Types
    "VARCHAR" -> StringType,
    "CHAR"    -> StringType,
    "BPCHAR"  -> StringType,
    "TEXT"    -> StringType, // Mapped to VARCHAR internally in Redshift
    "VARBYTE" -> BinaryType, // Binary data

    // Boolean Type
    "BOOLEAN" -> BooleanType,

    // Date/Time Types
    "DATE" -> DateType,
    // Redshift timestamps are often treated as microsecond precision by connectors.
    "TIMESTAMP"   -> TimestampType, // TIMESTAMP WITHOUT TIME ZONE
    "TIMESTAMPTZ" -> TimestampType, // TIMESTAMP WITH TIME ZONE

    // Specialized Types
    "GEOMETRY"  -> StringType, // Maps to WKT/GeoJson string in Spark
    "GEOGRAPHY" -> StringType,

    // Array/Complex Types (Redshift doesn't natively support arrays/structs in the same way)
    // These are often handled as delimited strings or JSON data if complex data is involved.
    // However, modern Redshift (via ARRAY/STRUCT types) is supported by newer connectors:
    "ARRAY" -> ArrayType(NullType), // Requires parsing the element type dynamically
    "SUPER" -> StringType // Semi-structured data type, often read as String or JSON
  ).map(kv => kv._1.toLowerCase -> kv._2.typeName) // Normalize keys to lowercase

  val mapOfMaps: Map[String, Map[String, String]] = Map(
    "bigquery"   -> bigqueryToSparkMap,
    "snowflake"  -> snowflakeToSparkMap,
    "duckdb"     -> duckDBToSparkMap,
    "postgresql" -> postgreSqlToSparkMap,
    "redshift"   -> redshiftToSparkMap
  )

  def getSparkType(
    sourceType: String,
    sqlDialect: String
  ): Option[String] = {
    if (sourceType.equalsIgnoreCase("variant")) { // until we migrate to Spark 4
      Some("variant")
    } else if (sqlDialect == "spark") {
      Some(sourceType.toLowerCase)
    } else {
      val dialectMap = mapOfMaps.get(sqlDialect.toLowerCase)
      dialectMap.flatMap(_.get(sourceType.toLowerCase)) match {
        case Some(value) => Some(value)
        case None        =>
          // Best effort
          val value = dialectMap.flatMap {
            _.toList
              .find { case (k, v) =>
                k.startsWith(sourceType.toLowerCase) || sourceType.toLowerCase.startsWith(
                  k.toLowerCase
                )
              }
              .map(_._2)
          }
          logger.warn(s"Best effort mapping for '$sourceType' in '$sqlDialect': $value")
          value
      }
    }
  }
}
