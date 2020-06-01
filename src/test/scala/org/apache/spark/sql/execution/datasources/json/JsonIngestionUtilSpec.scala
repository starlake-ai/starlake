package org.apache.spark.sql.execution.datasources.json

import com.ebiznext.comet.TestHelper
import org.apache.spark.sql.types.{StructField, _}

class JsonIngestionUtilSpec extends TestHelper {
  new WithSettings() {
    {
      val schemaType = (
        "root",
        StructType(
          Array(
            StructField("email", StringType, false),
            StructField("textattr", StringType, false),
            StructField("nullattr", StringType, true),
            StructField(
              "structAttr",
              StructType(
                Array(
                  StructField("intAttr", LongType, false),
                  StructField("decimalAttr", DecimalType(30, 15), true),
                  StructField("doubleAttr", DoubleType, true),
                  StructField("arrayOfString", ArrayType(StringType, true), true),
                  StructField("arrayOfInt", ArrayType(LongType, true), true)
                )
              ),
              true
            )
          )
        ),
        true
      )
      "exact match between message and schema" should "succeed" in {
        val datasetType = (
          "root",
          StructType(
            Array(
              StructField("email", StringType, true),
              StructField("textattr", StringType, true),
              StructField("nullattr", NullType, true),
              StructField(
                "structAttr",
                StructType(
                  Array(
                    StructField("intAttr", LongType, true),
                    StructField("decimalAttr", DoubleType, true),
                    StructField("doubleAttr", DoubleType, true),
                    StructField("arrayOfString", ArrayType(StringType, true), true),
                    StructField("arrayOfInt", ArrayType(LongType, true), true)
                  )
                ),
                true
              )
            )
          ),
          true
        )
        JsonIngestionUtil.compareTypes(Nil, schemaType, datasetType) should be('empty)
      }
      "field at the beginning of the message but not in the schema" should "produce an error" in {
        val datasetType = (
          "root",
          StructType(
            Array(
              StructField("invalidField1", LongType, true),
              StructField("email", StringType, true),
              StructField("textattr", StringType, true),
              StructField("nullattr", NullType, true),
              StructField(
                "structAttr",
                StructType(
                  Array(
                    StructField("intAttr", LongType, true),
                    StructField("decimalAttr", DoubleType, true),
                    StructField("doubleAttr", DoubleType, true),
                    StructField("arrayOfString", ArrayType(StringType, true), true),
                    StructField("arrayOfInt", ArrayType(LongType, true), true)
                  )
                ),
                true
              )
            )
          ),
          true
        )
        val errs = JsonIngestionUtil.compareTypes(Nil, schemaType, datasetType)
        errs.length should be > 0
      }
      "field at the end of the message but not in the schema" should "produce an error" in {
        val datasetType = (
          "root",
          StructType(
            Array(
              StructField("email", StringType, true),
              StructField("textattr", StringType, true),
              StructField("nullattr", NullType, true),
              StructField(
                "structAttr",
                StructType(
                  Array(
                    StructField("intAttr", LongType, true),
                    StructField("decimalAttr", DoubleType, true),
                    StructField("doubleAttr", DoubleType, true),
                    StructField("arrayOfString", ArrayType(StringType, true), true),
                    StructField("arrayOfInt", ArrayType(LongType, true), true),
                    StructField("invalidField2", LongType, true)
                  )
                ),
                true
              )
            )
          ),
          true
        )
        val errs = JsonIngestionUtil.compareTypes(Nil, schemaType, datasetType)
        errs.length should be > 0
      }
    }
  }
}
