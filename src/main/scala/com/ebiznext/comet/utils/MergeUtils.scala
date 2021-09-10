package com.ebiznext.comet.utils

import com.ebiznext.comet.schema.model.MergeOptions
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

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
    logger.info(s"incomingDF Schema before merge -> ${incomingDF.schema}")
    logger.info(s"existingDF Schema before merge -> ${existingDF.schema}")
    logger.info(s"existingDF field count=${existingDF.schema.fields.length}")
    logger.info(s"existingDF field list=${existingDF.schema.fields.map(_.name).mkString(",")}")
    logger.info(s"incomingDF field count=${incomingDF.schema.fields.length}")
    logger.info(s"incomingDF field list=${incomingDF.schema.fields.map(_.name).mkString(",")}")

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

  // return an optional list of column paths (ex "root.field" <=> ("root", "field"))
  private def findMissingColumnsType(
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

  private def supportsNestedFieldsOperations(dataFrame: DataFrame): Boolean =
    Version(dataFrame.sparkSession.sparkContext.version).compareTo(Version("3.1.0")) >= 0

  private def buildMissingType(
    dataframe: DataFrame,
    missingType: (List[String], DataType)
  ): DataFrame = buildMissingType(dataframe, missingType, supportsNestedFieldsOperations(dataframe))

  def buildMissingType(
    dataframe: DataFrame,
    missingType: (List[String], DataType),
    useNestedFields: Boolean
  ): DataFrame = {
    // Inspired from https://medium.com/@fqaiser94/manipulating-nested-data-just-got-easier-in-apache-spark-3-1-1-f88bc9003827
    def buildMissingColumn: (List[String], List[String], DataType, Boolean) => Column = {
      case (_ :+ colName, Nil, missingType, _) => lit(null).cast(missingType).as(colName)
      case (_ :+ colName, fields, missingType, true) =>
        col(colName).withField(fields.mkString("."), lit(null).cast(missingType))
      case (parents, colName :: fields, missingType, false) =>
        val parentColName = parents.mkString(".")
        when(
          col(parentColName).isNotNull,
          struct(
            col(s"$parentColName.*"),
            buildMissingColumn(parents ++ List(colName), fields, missingType, false)
          )
        )
      case (_, _, _, _) => throw new Exception("should never happen")
    }

    missingType match {
      case (colName :: tail, dataType) =>
        dataframe.withColumn(
          colName,
          buildMissingColumn(List(colName), tail, dataType, useNestedFields)
        )
      case (_, _) => dataframe
    }
  }

  /** Perform an union between two dataframe. Fixes any missing column from the originalDF by add
    * null values where needed. Assumes toAddDF to at least contains all the columns from originalDF
    * (see {@link computeCompatibleSchema})
    */
  private def computeDataframeUnion(originalDF: DataFrame, toAddDF: DataFrame): DataFrame = {
    val missingTypes = findMissingColumnsType(originalDF.schema, toAddDF.schema)
    val patchedDF = missingTypes
      .fold(originalDF) { missingTypes =>
        missingTypes.foldLeft(originalDF) { buildMissingType }
      }
      // Force ordering of columns to be the same
      .select(toAddDF.columns.map(col): _*)

    // place toAddDF first, so that if a new data takes precedence over the rest
    toAddDF.union(patchedDF)
  }
}
