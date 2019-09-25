package com.ebiznext.comet.schema.model

import org.apache.spark.sql.types.{DataType, StructField}

trait CometDataType
case class CometSimpleType(simpleType: DataType, attribute: Attribute, tpe: Type)
    extends CometDataType

case class CometStructField(sparkField: StructField, attribute: Attribute, tpe: Type)
    extends CometDataType

case class CometStructType(fields: Array[CometStructField]) extends CometDataType

case class CometArrayType(fields: CometStructType) extends CometDataType
