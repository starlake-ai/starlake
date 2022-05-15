package org.apache.spark.sql.execution.datasources.json

import ai.starlake.TestHelper
import org.apache.spark.sql.types.{StructField, _}

class JsonIngestionUtilSpec extends TestHelper {
  new WithSettings {
    {
      val schemaType = (
        "root",
        StructType(
          Array(
            StructField("email", StringType, nullable = false),
            StructField("textattr", StringType, nullable = false),
            StructField("nullattr", StringType, nullable = true),
            StructField(
              "structAttr",
              StructType(
                Array(
                  StructField("intAttr", LongType, nullable = false),
                  StructField("decimalAttr", DecimalType(38, 9), nullable = true),
                  StructField("doubleAttr", DoubleType, nullable = true),
                  StructField(
                    "arrayOfString",
                    ArrayType(StringType, containsNull = true),
                    nullable = true
                  ),
                  StructField(
                    "arrayOfInt",
                    ArrayType(LongType, containsNull = true),
                    nullable = true
                  )
                )
              ),
              nullable = true
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
              StructField("email", StringType, nullable = true),
              StructField("textattr", StringType, nullable = true),
              StructField("nullattr", NullType, nullable = true),
              StructField(
                "structAttr",
                StructType(
                  Array(
                    StructField("intAttr", LongType, nullable = true),
                    StructField("decimalAttr", DoubleType, nullable = true),
                    StructField("doubleAttr", DoubleType, nullable = true),
                    StructField(
                      "arrayOfString",
                      ArrayType(StringType, containsNull = true),
                      nullable = true
                    ),
                    StructField(
                      "arrayOfInt",
                      ArrayType(LongType, containsNull = true),
                      nullable = true
                    )
                  )
                ),
                nullable = true
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
              StructField("invalidField1", LongType, nullable = true),
              StructField("email", StringType, nullable = true),
              StructField("textattr", StringType, nullable = true),
              StructField("nullattr", NullType, nullable = true),
              StructField(
                "structAttr",
                StructType(
                  Array(
                    StructField("intAttr", LongType, nullable = true),
                    StructField("decimalAttr", DoubleType, nullable = true),
                    StructField("doubleAttr", DoubleType, nullable = true),
                    StructField(
                      "arrayOfString",
                      ArrayType(StringType, containsNull = true),
                      nullable = true
                    ),
                    StructField(
                      "arrayOfInt",
                      ArrayType(LongType, containsNull = true),
                      nullable = true
                    )
                  )
                ),
                nullable = true
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
              StructField("email", StringType, nullable = true),
              StructField("textattr", StringType, nullable = true),
              StructField("nullattr", NullType, nullable = true),
              StructField(
                "structAttr",
                StructType(
                  Array(
                    StructField("intAttr", LongType, nullable = true),
                    StructField("decimalAttr", DoubleType, nullable = true),
                    StructField("doubleAttr", DoubleType, nullable = true),
                    StructField(
                      "arrayOfString",
                      ArrayType(StringType, containsNull = true),
                      nullable = true
                    ),
                    StructField(
                      "arrayOfInt",
                      ArrayType(LongType, containsNull = true),
                      nullable = true
                    ),
                    StructField("invalidField2", LongType, nullable = true)
                  )
                ),
                nullable = true
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
