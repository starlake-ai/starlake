package ai.starlake.utils

import ai.starlake.TestHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.io.Source

class MergeUtilsTest extends TestHelper {
  new WithSettings() {
    "merging simple schema" should "succeed" in {
      val schema = StructType.fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
      val result = MergeUtils.computeCompatibleSchema(schema, schema)
      result shouldBe schema
    }

    "merging compatible schemas" should "succeed" in {
      val actualSchema = StructType.fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
      val compatibleSchema = StructType.fromDDL(
        "`root` STRUCT<`field`: BIGINT, `other`: STRING>,`note` STRING, `same` STRUCT<`value`: BIGINT>"
      )
      val result = MergeUtils.computeCompatibleSchema(actualSchema, compatibleSchema)
      result shouldBe actualSchema
    }

    "merging schemas with missing columns" should "succeed" in {
      val actualSchema = StructType.fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
      val invalidSchema = StructType.fromDDL("`root` STRUCT<`other`: BIGINT>,`note` STRING")

      val result = MergeUtils.computeCompatibleSchema(actualSchema, invalidSchema)
      result.sql shouldBe "STRUCT<root: STRUCT<>, note: STRING>"
    }

    "merging schemas new columns not nullable" should "fail" in {
      val actualSchema = StructType.fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
      val invalidSchema = StructType
        .fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
        .add("other", StringType, nullable = false)
      val computedSchema = MergeUtils.computeCompatibleSchema(actualSchema, invalidSchema)

      computedSchema should equal(actualSchema)
    }

    "build missing type with nested fields" should "succeed" in {
      val schema = StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT>")

      val dataFrame =
        sparkSession.read.schema(schema).json(getResPath("/sample/merge/existing.jsonl"))

      val newDataFrame = Map(List("field") -> StringType, List("data", "new") -> StringType)
        .foldLeft(dataFrame) { (dataframe, missingType) =>
          MergeUtils.buildMissingType(dataframe, missingType)
        }
      newDataFrame.schema shouldBe new StructType()
        .add("id", IntegerType)
        .add("data", new StructType().add("version", IntegerType).add("new", StringType))
        .add("field", StringType)

      val actual = newDataFrame.toJSON.collect()

      val stream = getClass.getResourceAsStream("/sample/merge/existing.jsonl")
      val expected = Source.fromInputStream(stream).getLines().toList
      actual should contain theSameElementsAs expected
    }

    "build missing type without nested fields" should "succeed" in {
      val schema = StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT>")

      val dataFrame =
        sparkSession.read.schema(schema).json(getResPath("/sample/merge/existing.jsonl"))

      val newDataFrame = Map(List("field") -> StringType, List("data", "new") -> StringType)
        .foldLeft(dataFrame) { (dataframe, missingType) =>
          MergeUtils.buildMissingType(dataframe, missingType)
        }
      newDataFrame.schema shouldBe new StructType()
        .add("id", IntegerType)
        .add("data", new StructType().add("version", IntegerType).add("new", StringType))
        .add("field", StringType)

      val actual = newDataFrame.toJSON.collect()

      val stream = getClass.getResourceAsStream("/sample/merge/existing.jsonl")
      val expected = Source.fromInputStream(stream).getLines().toList
      actual should contain theSameElementsAs expected
    }

    "Merge function " should "be merge structure information" in {
      val incomingSchema1 = StructType(
        Array(
          StructField("field1", StringType).withComment("description field 1"),
          StructField("field2", IntegerType).withComment("description field 2"),
          StructField("field3", StringType).withComment("description field 3")
        )
      )
      val existingSchema1 = StructType(
        Array(
          StructField("field1", StringType).withComment("description field 1"),
          StructField("field2", LongType).withComment("description field 2")
        )
      )

      val incomingDf1 = sparkSession.createDataFrame(
        Seq(Row("1", 10, "val1"), Row("2", 20, "val2")).asJava,
        incomingSchema1
      )
      val existingDf1 = sparkSession.createDataFrame(
        Seq(Row("1", 100L), Row("4", 40L)).asJava,
        existingSchema1
      )
    }
  }
}
