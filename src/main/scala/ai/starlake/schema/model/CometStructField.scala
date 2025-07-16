package ai.starlake.schema.model

import org.apache.spark.sql.types.{DataType, StructField}

trait CometDataType

case class CometSimpleType(simpleType: DataType, attribute: TableAttribute, tpe: Type)
    extends CometDataType

case class CometStructField(sparkField: StructField, attribute: TableAttribute, tpe: Type)
    extends CometDataType

case class CometStructType(fields: Array[CometStructField]) extends CometDataType

case class CometArrayType(fields: CometStructType) extends CometDataType
