package ai.starlake.schema.model

/** Starlake's internal schema representation, independent of any execution engine (Spark, DuckDB,
  * etc.). This replaces the previous dependency on org.apache.spark.sql.types.StructType.
  */

sealed trait StarlakeDataType {
  def typeName: String
}

object StarlakeDataType {
  case object SLString extends StarlakeDataType { val typeName = "string" }
  case object SLLong extends StarlakeDataType { val typeName = "long" }
  case object SLInt extends StarlakeDataType { val typeName = "integer" }
  case object SLShort extends StarlakeDataType { val typeName = "short" }
  case object SLByte extends StarlakeDataType { val typeName = "byte" }
  case object SLDouble extends StarlakeDataType { val typeName = "double" }
  case object SLFloat extends StarlakeDataType { val typeName = "float" }
  case object SLBoolean extends StarlakeDataType { val typeName = "boolean" }
  case object SLBinary extends StarlakeDataType { val typeName = "binary" }
  case object SLVariant extends StarlakeDataType { val typeName = "variant" }

  // Date type
  case object SLDate extends StarlakeDataType { val typeName = "date" }

  // Timestamp variants
  /** Standard timestamp (with timezone in DuckDB/Spark). Maps to:
    *   - Spark: TimestampType
    *   - DuckDB: TIMESTAMP or TIMESTAMPTZ
    *   - Snowflake: TIMESTAMP_LTZ or TIMESTAMP_TZ
    *   - PostgreSQL: TIMESTAMPTZ
    *   - BigQuery: TIMESTAMP
    */
  case object SLTimestamp extends StarlakeDataType { val typeName = "timestamp" }

  /** Timestamp without timezone. Maps to:
    *   - Spark: TimestampNTZType
    *   - DuckDB: TIMESTAMP (without TZ)
    *   - Snowflake: TIMESTAMP_NTZ
    *   - PostgreSQL: TIMESTAMP (without TZ)
    *   - BigQuery: DATETIME
    */
  case object SLTimestampNTZ extends StarlakeDataType { val typeName = "timestamp_ntz" }

  /** Time-only type (no date component). Maps to:
    *   - Spark: StringType (no native TIME support)
    *   - DuckDB: TIME
    *   - Snowflake: TIME
    *   - PostgreSQL: TIME / TIMETZ
    *   - BigQuery: TIME
    */
  case object SLTime extends StarlakeDataType { val typeName = "time" }

  case class SLDecimal(precision: Int = 38, scale: Int = 9) extends StarlakeDataType {
    val typeName = s"decimal($precision,$scale)"
  }

  case class SLStruct(fields: Seq[StarlakeField]) extends StarlakeDataType {
    val typeName = "struct"
    def fieldNames: Seq[String] = fields.map(_.name)
    def apply(name: String): StarlakeField =
      fields.find(_.name == name).getOrElse(
        throw new IllegalArgumentException(s"Field $name not found in struct")
      )
  }

  case class SLArray(elementType: StarlakeDataType) extends StarlakeDataType {
    val typeName = s"array<${elementType.typeName}>"
  }
}

case class StarlakeField(
  name: String,
  dataType: StarlakeDataType,
  nullable: Boolean = true,
  comment: Option[String] = None
) {
  def withComment(c: String): StarlakeField =
    if (c.isEmpty) this else copy(comment = Some(c))
}

case class StarlakeSchema(fields: Seq[StarlakeField]) {
  def fieldNames: Seq[String] = fields.map(_.name)

  def apply(name: String): StarlakeField =
    fields.find(_.name == name).getOrElse(
      throw new IllegalArgumentException(s"Field $name not found in schema")
    )

  def add(field: StarlakeField): StarlakeSchema = StarlakeSchema(fields :+ field)

  def isFlat: Boolean = !fields.exists(_.dataType.isInstanceOf[StarlakeDataType.SLStruct])
}