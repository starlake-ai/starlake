package com.ebiznext.comet.utils

import org.apache.spark.sql.types._

object DataTypeEx {

  implicit class DataTypeEx(dataType: DataType) {

    def isOfValidDiscreteType() =
      dataType match {
        case StringType | LongType | IntegerType | ShortType | DoubleType | BooleanType |
            ByteType =>
          true
        case _ =>
          false
      }

    def isOfValidContinuousType() =
      dataType match {
        case LongType | IntegerType | ShortType | DoubleType | ByteType =>
          true
        case _ =>
          false
      }

  }
}
