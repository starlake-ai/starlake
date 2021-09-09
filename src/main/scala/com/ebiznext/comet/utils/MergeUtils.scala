package com.ebiznext.comet.utils

import com.ebiznext.comet.schema.model.MergeOptions
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, date_format, lit, row_number}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

object MergeUtils extends StrictLogging {

  /** Compute a new schema that is compatible with merge operations. Built recursively from the
    * incoming schema to retain the latest attributes, but without the columns that does not exist
    * yet. Ensures that the incomingSchema contains all the columns from the existingSchema.
    */
  def computeCompatibleSchema(actualSchema: StructType, expectedSchema: StructType): StructType = {
    val actualColumns = actualSchema.map(field => field.name -> field).toMap
    val expectedColumns = expectedSchema.map(field => field.name -> field).toMap

    val missingColumns = actualColumns.keySet.diff(expectedColumns.keySet)
    if (missingColumns.nonEmpty)
      throw new RuntimeException(
        "Input Dataset should contain every column from the existing HDFS dataset. The following columns were not matched: " + missingColumns
          .mkString(", ")
      )

    val newColumns = expectedColumns.keySet.diff(actualColumns.keySet)
    val newColumnsNotNullable =
      newColumns.flatMap(expectedColumns.get).filterNot(_.nullable).map(_.name)
    if (newColumnsNotNullable.nonEmpty)
      throw new RuntimeException(
        "The new columns from Input Dataset should be nullable. The following columns were not: " + newColumnsNotNullable
          .mkString(", ")
      )

    StructType(
      expectedSchema
        .flatMap(expectedField =>
          actualColumns
            .get(expectedField.name)
            .map(existingField =>
              (existingField.dataType, expectedField.dataType) match {
                case (existingType: StructType, incomingType: StructType) =>
                  expectedField.copy(dataType = computeCompatibleSchema(existingType, incomingType))
                case (
                      ArrayType(existingType: StructType, _),
                      ArrayType(incomingType: StructType, nullable)
                    ) =>
                  expectedField
                    .copy(dataType =
                      ArrayType(computeCompatibleSchema(existingType, incomingType), nullable)
                    )
                case (_, _) => expectedField
              }
            )
        )
    )
  }

  def computeToMergeAndToDeleteDF(
    existingDF: DataFrame,
    incomingDF: DataFrame,
    mergeOptions: MergeOptions
  ): (DataFrame, DataFrame) = {
    val finalIncomingDF = mergeOptions.delete
      .map(condition => incomingDF.filter(s"not ($condition)"))
      .getOrElse(incomingDF)

    val orderingWindow = Window
      .partitionBy(mergeOptions.key.head, mergeOptions.key.tail: _*)
      .orderBy(mergeOptions.timestamp.fold(mergeOptions.key) { List(_) }.map(col(_).desc): _*)

    val allRowsDF = computeDataframeUnion(existingDF, finalIncomingDF)

    val allRowsWithRownum = allRowsDF
      .withColumn("rownum", row_number.over(orderingWindow))

    // Deduplicate
    val mergedDF = allRowsWithRownum
      .where(col("rownum") === 1)
      .drop("rownum")

    // Compute rows that will be deleted
    val toDeleteDF = allRowsWithRownum
      .where(col("rownum") =!= 1)
      .drop("rownum")

    logger.whenDebugEnabled {
      logger.debug(s"Merge detected ${toDeleteDF.count()} items to update/delete")
      logger.debug(s"Merge detected ${mergedDF.except(finalIncomingDF).count()} items to insert")
      mergedDF.show(false)
    }

    (mergedDF, toDeleteDF)
  }

  /** Perform an union between two dataframe. Fixes any missing column from the originalDF by add
    * null values where needed. Assumes toAddDF to at least contains all the columns from originalDF
    * (see {@link computeCompatibleSchema})
    */
  private def computeDataframeUnion(originalDF: DataFrame, toAddDF: DataFrame): DataFrame = {
    // return an optional list of column paths (ex "root.field" <=> ("root", "field"))
    def findMissingColumnsType(
      schema: StructType,
      reference: StructType,
      stack: List[String] = List()
    ): Option[Map[List[String], DataType]] = {
      val fields = schema.fields.map(field => field.name -> field).toMap
      reference.fields
        .flatMap { referenceField =>
          fields
            .get(referenceField.name)
            .fold(
              // Without match, we have found a missing column
              Option(Map((stack :+ referenceField.name) -> referenceField.dataType))
            ) { field =>
              (field.dataType, referenceField.dataType) match {
                // In here, we known the reference column is matched in the original schema
                // If the innerType is a StructType, we must do some recursion
                // Otherwise, we can end the search, there is no missing column here.
                case (fieldType: StructType, referenceType: StructType) =>
                  findMissingColumnsType(fieldType, referenceType, stack :+ referenceField.name)
                case (
                      ArrayType(fieldType: StructType, _),
                      ArrayType(referenceType: StructType, _)
                    ) =>
                  findMissingColumnsType(fieldType, referenceType, stack :+ referenceField.name)
                case (_, _) => None
              }
            }
        }
        .reduceOption(_ ++ _)
    }

    val missingTypes = findMissingColumnsType(originalDF.schema, toAddDF.schema)
    val patchedDF = missingTypes
      .fold(originalDF) { missingTypes =>
        missingTypes.foldLeft(originalDF) {
          (dataframe: DataFrame, missingType: (List[String], DataType)) =>
            missingType match {
              case (Nil, _) => dataframe
              case (colName :: Nil, dataType) =>
                dataframe.withColumn(colName, lit(null).cast(dataType))
              case (colName :: tail, dataType) =>
                dataframe.withColumn(
                  colName,
                  col(colName).withField(tail.mkString("."), lit(null).cast(dataType))
                )
            }
        }
      }
      // Force ordering of columns to be the same
      .select(toAddDF.columns.map(col): _*)

    // place toAddDF first, so that if a new data takes precedence over the rest
    toAddDF.union(patchedDF)
  }

  def computePartitionsToUpdateAfterMerge(
    mergedDF: DataFrame,
    toDeleteDF: DataFrame,
    timestamp: String,
    dateFormat: String
  ): List[String] = {
    logger.info(s"Computing partitions to update on date column $timestamp")
    val partitionsToUpdate = mergedDF
      .select(col(timestamp))
      .union(toDeleteDF.select(col(timestamp)))
      .select(date_format(col(timestamp), dateFormat).cast("string"))
      .where(col(timestamp).isNotNull)
      .distinct()
      .collect()
      .map(_.getString(0))
      .toList
    logger.info(
      s"The following partitions will be updated ${partitionsToUpdate.mkString(",")}"
    )
    partitionsToUpdate
  }

}
