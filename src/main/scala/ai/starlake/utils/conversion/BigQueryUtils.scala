package ai.starlake.utils.conversion

import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{FieldValue, LegacySQLTypeName, Schema => BQSchema, TableId}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types._

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

  def normalizeCompatibleSchema(schema: StructType, existingSchema: StructType): StructType = {
    val existingFields = existingSchema.fields.map { field => field.name -> field }.toMap
    val fields = schema.fields.map { field =>
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
    schema.copy(fields = fields)
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
          field.getBytesValue
        case LegacySQLTypeName.STRING =>
          field.getStringValue
        case LegacySQLTypeName.INTEGER =>
          field.getLongValue
        case LegacySQLTypeName.FLOAT =>
          field.getDoubleValue
        case LegacySQLTypeName.NUMERIC =>
          field.getNumericValue
        case LegacySQLTypeName.BIGNUMERIC =>
          field.getNumericValue
        case LegacySQLTypeName.BOOLEAN =>
          field.getBooleanValue
        case LegacySQLTypeName.TIMESTAMP =>
          field.getTimestampValue
        case LegacySQLTypeName.DATE =>
          field.getStringValue
        case LegacySQLTypeName.GEOGRAPHY =>
          field.getStringValue
        case LegacySQLTypeName.TIME =>
          field.getStringValue
        case LegacySQLTypeName.DATETIME =>
          field.getTimestampValue
        case LegacySQLTypeName.RECORD =>
          field.getRecordValue
        case LegacySQLTypeName.JSON =>
          field.getStringValue
        case LegacySQLTypeName.INTERVAL =>
          field.getPeriodDuration
        case _ =>
          field
      }
    }
  }
  def tableIdToString(tableId: TableId) = {
    (tableId.getProject, tableId.getDataset, tableId.getTable) match {
      case (null, null, null)        => ""
      case (null, null, table)       => table
      case (null, dataset, table)    => s"$dataset.$table"
      case (project, dataset, table) => s"`$project`.$dataset.$table"
    }
  }
}
