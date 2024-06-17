package ai.starlake.utils.conversion

import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{FieldValue, LegacySQLTypeName, Schema => BQSchema, TableId}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types._
import org.threeten.extra.PeriodDuration

/** [X] whatever Conversion between [X] Schema and BigQuery Schema
  */
object BigQueryUtils {

  val sparkToBq: DataFrame => BQSchema = (df: DataFrame) => bqSchema(df.schema)

  /** Compute BigQuery Schema from Spark or PArquet Schema while Schema.bqSchema compute it from YMl
    * File
    * @param schema
    *   Spark DataType
    * @return
    */

  def bqSchema(schema: DataType): BQSchema = {
    BigQuerySchemaConverters.toBigQuerySchema(schema.asInstanceOf[StructType])
  }

  /** Spark BigQuery driver consider integer in BQ as Long. We need to convert the Int DataType to
    * LongType before loading the data. As a good practice, always use long when dealing with big
    * query in your YAML Schema.
    * @param schema
    * @return
    */
  def normalizeSchema(schema: StructType): StructType = {
    val fields = schema.fields.map { field =>
      field.dataType match {
        case dataType: StructType =>
          field.copy(dataType = normalizeSchema(dataType))
        case ArrayType(elementType: StructType, nullable) =>
          field.copy(dataType = ArrayType(normalizeSchema(elementType), nullable))
        case ArrayType(_: IntegerType, nullable) =>
          field.copy(dataType = ArrayType(LongType, nullable))
        case IntegerType => field.copy(dataType = LongType)
        case _           => field
      }
    }
    schema.copy(fields = fields)
  }

  def normalizeCompatibleSchema(
    incomingSchema: StructType,
    existingSchema: StructType
  ): StructType = {
    val existingFields = existingSchema.fields.map { field => field.name -> field }.toMap
    val incomingFields = incomingSchema.fields.map { field =>
      val typedField = field.dataType match {
        case dataType: StructType =>
          field.copy(dataType = normalizeSchema(dataType))
        case ArrayType(elementType: StructType, nullable) =>
          field.copy(dataType = ArrayType(normalizeSchema(elementType), nullable))
        case ArrayType(_: IntegerType, nullable) =>
          field.copy(dataType = ArrayType(LongType, nullable))
        case IntegerType => field.copy(dataType = LongType)
        case _           => field
      }
      existingFields.get(typedField.name) match {
        case Some(existingField) =>
          if (existingField.nullable && !typedField.nullable)
            typedField.copy(nullable = true)
          else
            typedField
        case None =>
          typedField
      }
    }
    incomingSchema.copy(fields = incomingFields)
  }

  def computePartitionsToUpdateAfterMerge(
    mergedDF: DataFrame,
    toDeleteDF: DataFrame,
    timestamp: String
  ): List[String] = {
    mergedDF
      .select(col(timestamp))
      .union(toDeleteDF.select(col(timestamp)))
      .select(date_format(col(timestamp), "yyyyMMdd").cast("string"))
      .where(col(timestamp).isNotNull)
      .distinct()
      .collect()
      .map(_.getString(0))
      .toList
  }

  def anyRefToAny(fields: Seq[FieldValue], schema: Seq[LegacySQLTypeName]): Seq[Any] = {
    val fieldsWithSchema = fields.zip(schema)
    fieldsWithSchema.map { case (field, schema) =>
      schema match {
        case LegacySQLTypeName.BYTES =>
          Option(field).map(_.getBytesValue).getOrElse(Array[Byte]())
        case LegacySQLTypeName.STRING =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.INTEGER =>
          Option(field).map(_.getLongValue).getOrElse(0L)
        case LegacySQLTypeName.FLOAT =>
          Option(field).map(_.getDoubleValue).getOrElse(0.0)
        case LegacySQLTypeName.NUMERIC =>
          Option(field).map(_.getNumericValue).getOrElse(BigDecimal(0))
        case LegacySQLTypeName.BIGNUMERIC =>
          Option(field).map(_.getNumericValue).getOrElse(BigDecimal(0))
        case LegacySQLTypeName.BOOLEAN =>
          Option(field).map(_.getBooleanValue).getOrElse(false)
        case LegacySQLTypeName.TIMESTAMP =>
          Option(field).map(_.getTimestampValue).getOrElse(0L)
        case LegacySQLTypeName.DATE =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.GEOGRAPHY =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.TIME =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.DATETIME =>
          Option(field).map(_.getTimestampValue).getOrElse(0L)
        case LegacySQLTypeName.RECORD =>
          field.getRecordValue()
        case LegacySQLTypeName.JSON =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.INTERVAL =>
          Option(field).map(_.getPeriodDuration).getOrElse(PeriodDuration.ZERO)
        case _ =>
          field
      }
    }
  }

  def tableIdToTableName(tableId: TableId): String = {
    (tableId.getProject, tableId.getDataset, tableId.getTable) match {
      case (null, null, null)        => ""
      case (null, null, table)       => table
      case (null, dataset, table)    => s"$dataset.$table"
      case (project, dataset, table) => s"`$project`.$dataset.$table"
    }
  }
}
