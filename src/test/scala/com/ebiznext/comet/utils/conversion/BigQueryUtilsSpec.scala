package com.ebiznext.comet.utils.conversion

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class BigQueryUtilsSpec extends TestHelper {
  new WithSettings() {
    val sparkEnv: SparkEnv = new SparkEnv("test")
    val session: SparkSession = sparkEnv.session

    "category" should "work" in {

      val schema = StructType(
        Seq(
          StructField("categoryId", StringType, true),
          StructField("categorySynonyms", ArrayType(StringType, true), true),
          StructField("categoryUrl", StringType, true),
          StructField("exclusiveOfferCode", StringType, true),
          StructField(
            "filters",
            ArrayType(
              StructType(
                Seq(
                  StructField("dimension", StringType, true)
//                  StructField(
//                    "dimensionValues",
//                    ArrayType(
//                      StructType(
//                        Seq(
//                          StructField("identifier", StringType, true),
//                          StructField("label", StringType, true),
//                          StructField("mediaUrl", StringType, true),
//                          StructField("previewUrl", StringType, true)
//                        )
//                      ),
//                      true
//                    ),
//                    true
//                  ),
//                  StructField("name", StringType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("name", StringType, true),
          StructField("products", ArrayType(StringType, true), true),
          StructField("subCategories", ArrayType(StringType, true), true)
        )
      )
      val bqSchema = BigQueryUtils.bqSchema(
        sparkSession.read
          .json("/Users/elarib/Downloads/ingesting_digital_fr_CA_20201111.json")
          .schema
      )

      println(bqSchema)

    }

//    "Spark Types" should "be converted to corresponding BQ Types" in {
//      val res: BQSchema = List(
//        (
//          1,
//          true,
//          2.5,
//          "hello",
//          'x'.asInstanceOf[Byte],
//          new Date(System.currentTimeMillis()),
//          new Timestamp(System.currentTimeMillis())
//        )
//      ).toDF().to[BQSchema]
//      //Schema{fields=[Field{name=value, type=INTEGER, mode=NULLABLE, description=, policyTags=null}]}
//      val fields =
//        List(
//          Field
//            .newBuilder("_1", StandardSQLTypeName.INT64)
//            .setDescription("")
//            .setMode(Field.Mode.NULLABLE)
//            .build(),
//          Field
//            .newBuilder("_2", StandardSQLTypeName.BOOL)
//            .setDescription("")
//            .setMode(Field.Mode.NULLABLE)
//            .build(),
//          Field
//            .newBuilder("_3", StandardSQLTypeName.FLOAT64)
//            .setDescription("")
//            .setMode(Field.Mode.NULLABLE)
//            .build(),
//          Field
//            .newBuilder("_4", StandardSQLTypeName.STRING)
//            .setDescription("")
//            .setMode(Field.Mode.NULLABLE)
//            .build(),
//          Field
//            .newBuilder("_5", StandardSQLTypeName.INT64)
//            .setDescription("")
//            .setMode(Field.Mode.NULLABLE)
//            .build(),
//          Field
//            .newBuilder("_6", StandardSQLTypeName.DATE)
//            .setDescription("")
//            .setMode(Field.Mode.NULLABLE)
//            .build(),
//          Field
//            .newBuilder("_7", StandardSQLTypeName.TIMESTAMP)
//            .setDescription("")
//            .setMode(Field.Mode.NULLABLE)
//            .build()
//        )
//      res.getFields should contain theSameElementsInOrderAs fields
//    }
  }
}
