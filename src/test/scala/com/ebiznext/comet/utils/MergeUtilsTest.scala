package com.ebiznext.comet.utils

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model.MergeOptions
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.io.Source

class MergeUtilsTest extends TestHelper {

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

  "merging schemas with missing columns" should "fail" in {
    val actualSchema = StructType.fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
    val invalidSchema = StructType.fromDDL("`root` STRUCT<`other`: BIGINT>,`note` STRING")

    assertThrows[RuntimeException] {
      MergeUtils.computeCompatibleSchema(actualSchema, invalidSchema)
    }
  }

  "merging schemas new columns not nullable" should "fail" in {
    val actualSchema = StructType.fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
    val invalidSchema = StructType
      .fromDDL("`root` STRUCT<`field`: BIGINT>,`note` STRING")
      .add("other", StringType, nullable = false)

    assertThrows[RuntimeException] {
      MergeUtils.computeCompatibleSchema(actualSchema, invalidSchema)
    }
  }

  "merging two dataset with duplicated lines" should "deduplicate the lines" in {
    val schema = StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT>")
    val existingDF =
      sparkSession.read.schema(schema).json(getResPath("/sample/merge/existing.jsonl"))
    val incomingDF =
      sparkSession.read.schema(schema).json(getResPath("/sample/merge/incoming.jsonl"))

    val (mergedDF, _) =
      MergeUtils.computeToMergeAndToDeleteDF(existingDF, incomingDF, MergeOptions(key = List("id")))
    val actual = mergedDF.toJSON.collect()

    val expected = Source.fromResource("expected/merge/merge-simple.jsonl").getLines().toList
    actual should contain theSameElementsAs expected
  }

  "merging two dataset with duplicated lines using timestamp" should "keep the latest line of each" in {
    val schema = StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT>")
    val existingDF =
      sparkSession.read.schema(schema).json(getResPath("/sample/merge/existing.jsonl"))
    val incomingDF =
      sparkSession.read.schema(schema).json(getResPath("/sample/merge/incoming.jsonl"))

    val (mergedDF, _) = MergeUtils.computeToMergeAndToDeleteDF(
      existingDF,
      incomingDF,
      MergeOptions(key = List("id"), timestamp = Some("data.version"))
    )
    val actual = mergedDF.toJSON.collect()

    val expected =
      Source.fromResource("expected/merge/merge-with-timestamp.jsonl").getLines().toList
    actual should contain theSameElementsAs expected
  }

  "merging two dataset with duplicated lines and different schemas" should "deduplicate the lines" in {
    val existingSchema = StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT>")
    val incomingSchema =
      StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT,`new`: STRING>,`field` STRING")
    val existingDF =
      sparkSession.read.schema(existingSchema).json(getResPath("/sample/merge/existing.jsonl"))
    val incomingDF = sparkSession.read
      .schema(incomingSchema)
      .json(getResPath("/sample/merge/incoming-new-schema.jsonl"))

    val (mergedDF, _) =
      MergeUtils.computeToMergeAndToDeleteDF(existingDF, incomingDF, MergeOptions(key = List("id")))
    val actual = mergedDF.toJSON.collect()

    val expected = Source.fromResource("expected/merge/merge-new-schema.jsonl").getLines().toList
    actual should contain theSameElementsAs expected
  }

  "build missing type with nested fields" should "succeed" in {
    val schema = StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT>")

    val dataFrame =
      sparkSession.read.schema(schema).json(getResPath("/sample/merge/existing.jsonl"))

    val newDataFrame = Map(List("field") -> StringType, List("data", "new") -> StringType)
      .foldLeft(dataFrame) { (dataframe, missingType) =>
        MergeUtils.buildMissingType(dataframe, missingType, useNestedFields = true)
      }
    newDataFrame.schema shouldBe new StructType()
      .add("id", IntegerType)
      .add("data", new StructType().add("version", IntegerType).add("new", StringType))
      .add("field", StringType)

    val actual = newDataFrame.toJSON.collect()

    val expected = Source.fromResource("sample/merge/existing.jsonl").getLines().toList
    actual should contain theSameElementsAs expected
  }

  "build missing type without nested fields" should "succeed" in {
    val schema = StructType.fromDDL("`id` INT,`data` STRUCT<`version`: INT>")

    val dataFrame =
      sparkSession.read.schema(schema).json(getResPath("/sample/merge/existing.jsonl"))

    val newDataFrame = Map(List("field") -> StringType, List("data", "new") -> StringType)
      .foldLeft(dataFrame) { (dataframe, missingType) =>
        MergeUtils.buildMissingType(dataframe, missingType, useNestedFields = false)
      }
    newDataFrame.schema shouldBe new StructType()
      .add("id", IntegerType)
      .add("data", new StructType().add("version", IntegerType).add("new", StringType))
      .add("field", StringType)

    val actual = newDataFrame.toJSON.collect()

    val expected = Source.fromResource("sample/merge/existing.jsonl").getLines().toList
    actual should contain theSameElementsAs expected
  }
}
