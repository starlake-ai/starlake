package ai.starlake.utils.conversion

import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  Metadata => SparkMetadata,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampType,
  VarcharType
}

object DuckDbUtils {
  def bqSchema(schema: DataType): Array[String] = {
    sparkToDuckDbFields(schema.asInstanceOf[StructType], 0)
  }

  private def sparkToDuckDbFields(struct: StructType, level: Int): Array[String] = {
    val fields = struct.fields.map { field =>
      createDuckDbColumn(field, level)
    }
    fields
  }

  private def createDuckDbColumn(field: StructField, level: Int): String = {
    val dataType = field.dataType
    val name = field.name
    val nullable = if (!field.nullable) "NOT NULL" else ""
    val description = getDescription(field).map(d => s"COMMENT '$d'").getOrElse("")
    val (elementType, repeated) =
      dataType match {
        case arrayType: ArrayType =>
          (arrayType.elementType, true)
        case _ =>
          (dataType, false)
      }

    val column =
      elementType match {
        case struct: StructType =>
          val fields = sparkToDuckDbFields(struct, level + 1)
          if (repeated) {
            s"$name STRUCT(${fields.mkString("", ", ", "")})[]"
          } else {
            s"$name STRUCT(${fields.mkString("", ", ", "")})"
          }
        case decimal: DecimalType =>
          if (repeated) {
            s"$name DECIMAL(${decimal.precision},${decimal.scale})[]"
          } else {
            s"$name DECIMAL(${decimal.precision},${decimal.scale})"
          }
        case _ =>
          if (repeated) {
            s"$name ${toDuckDbType(elementType, field.metadata)}[]"
          } else
            s"$name ${toDuckDbType(elementType, field.metadata)}"
      }
    if (level > 0) column.trim
    else
      s"$column $nullable $description".trim

  }

  private def getDescription(field: StructField): Option[String] = {
    val comment =
      if (!field.getComment().isEmpty) {
        field.getComment()
      } else if (field.metadata.contains("description")) {
        Option(field.metadata.getString("description"))
      } else {
        None
      }
    comment
  }

  def isJson(metadata: SparkMetadata): Boolean =
    metadata.contains("sqlType") && "JSON" == metadata.getString("sqlType")

  def toDuckDbType(dataType: DataType, metadata: SparkMetadata): String = {
    dataType match {
      case _: BinaryType    => "BINARY"
      case _: ByteType      => "TINYINT"
      case _: ShortType     => "SMALLINT"
      case _: IntegerType   => "INTEGER"
      case _: LongType      => "BIGINT"
      case _: BooleanType   => "BOOLEAN"
      case _: FloatType     => "FLOAT"
      case _: DoubleType    => "DOUBLE"
      case _: StringType    => if (isJson(metadata)) "JSON" else "VARCHAR"
      case _: VarcharType   => if (isJson(metadata)) "JSON" else "VARCHAR"
      case _: TimestampType => "TIMESTAMP"
      case _: DateType      => "DATE"
      case _ =>
        throw new IllegalArgumentException("Data type not expected: " + dataType.simpleString)
    }
  }
}
