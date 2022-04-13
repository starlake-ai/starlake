package ai.starlake.utils

import ai.starlake.schema.model.MergeOptions
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, DatasetLogging}

object MergeUtils extends StrictLogging with DatasetLogging {

  /** Compute a new schema that is compatible with merge operations. Built recursively from the
    * incoming schema to retain the latest attributes, but without the columns that does not exist
    * yet. Ensures that the incomingSchema contains all the columns from the existingSchema.
    */
  def computeCompatibleSchema(actualSchema: StructType, expectedSchema: StructType): StructType = {
    val actualColumns = actualSchema.map(field => field.name -> field).toMap
    val expectedColumns = expectedSchema.map(field => field.name -> field).toMap

    val missingColumns = actualColumns.keySet.diff(expectedColumns.keySet)
    if (missingColumns.nonEmpty) {
      logger.info(s"Columns omitted in the new schema: ${missingColumns.mkString(",")}")
      val missingColumnsNotNullable = missingColumns.filterNot(actualColumns(_).nullable)
      if (missingColumnsNotNullable.nonEmpty)
        throw new RuntimeException(
          s"Input Dataset should contain every required column from the existing HDFS dataset. The following columns were not matched: ${missingColumnsNotNullable
              .mkString(",")}"
        )
    }

    val newColumns = expectedColumns.keySet.diff(actualColumns.keySet)
    val newColumnsNotNullable =
      newColumns.flatMap(expectedColumns.get(_)).filterNot(_.nullable).map(_.name)
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

  /** @param existingDF
    * @param incomingDF
    * @param mergeOptions
    * @return
    *   a tuple containing incomingDF without de records to delete as specified in the merge.delete
    *   option, the merge Dataframe containing all the data and deleted DF containing all the rows
    *   to delete
    */
  def computeToMergeAndToDeleteDF(
    existingDF: DataFrame,
    incomingDF: DataFrame,
    mergeOptions: MergeOptions
  ): (DataFrame, DataFrame, DataFrame) = {
    logger.whenInfoEnabled {
      logger.info(s"incomingDF Schema before merge -> ${incomingDF.schema}")
      logger.info(s"existingDF Schema before merge -> ${existingDF.schema}")
      logger.info(s"existingDF field count=${existingDF.schema.fields.length}")
      logger.info(s"existingDF field list=${existingDF.schema.fieldNames.mkString(",")}")
      logger.info(s"incomingDF field count=${incomingDF.schema.fields.length}")
      logger.info(s"incomingDF field list=${incomingDF.schema.fieldNames.mkString(",")}")
    }

    val finalIncomingDF = mergeOptions.delete
      .map(condition => incomingDF.filter(s"not ($condition)"))
      .getOrElse(incomingDF)

    val (mergedDF, toDeleteDF) = mergeOptions.timestamp match {
      case Some(timestamp) =>
        // We only keep the first occurrence of each record, from both datasets
        val orderingWindow = Window
          .partitionBy(mergeOptions.key.head, mergeOptions.key.tail: _*)
          .orderBy(col(timestamp).desc)

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
        (mergedDF, toDeleteDF)
      case None =>
        // We directly remove from the existing dataset the rows that are present in the incoming dataset
        val patchedExistingDF = addMissingAttributes(existingDF, finalIncomingDF)
        val patchedIncomingDF = addMissingAttributes(finalIncomingDF, existingDF)
        val commonDF = patchedExistingDF
          .join(patchedIncomingDF.select(mergeOptions.key.map(col): _*), mergeOptions.key)
          .select(patchedIncomingDF.columns.map(col): _*)
        (patchedExistingDF.except(commonDF).union(patchedIncomingDF), commonDF)
    }

    logger.whenDebugEnabled {
      logger.debug(s"Merge detected ${toDeleteDF.count()} items to update/delete")
      logger.debug(s"Merge detected ${mergedDF.except(finalIncomingDF).count()} items to insert")
      logger.debug(mergedDF.showString(truncate = 0))
    }

    (finalIncomingDF, mergedDF, toDeleteDF)
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

  def buildMissingType(
    dataframe: DataFrame,
    missingType: (List[String], DataType)
  ): DataFrame = {
    // Inspired from https://medium.com/@fqaiser94/manipulating-nested-data-just-got-easier-in-apache-spark-3-1-1-f88bc9003827
    def buildMissingColumn: (List[String], List[String], DataType) => Column = {
      case (_ :+ colName, Nil, missingType) => lit(null).cast(missingType).as(colName)
      // TODO Once we drop support for Spark 2. Use directly withField instead
      // case (_ :+ colName, fields, missingType, true) =>
      //  col(colName).withField(fields.mkString("."), lit(null).cast(missingType))
      // case (parents, colName :: fields, missingType, false) =>
      case (parents, colName :: fields, missingType) =>
        val parentColName = parents.mkString(".")
        when(
          col(parentColName).isNotNull,
          struct(
            col(s"$parentColName.*"),
            buildMissingColumn(parents ++ List(colName), fields, missingType)
          )
        )
      case (_, _, _) => throw new Exception("should never happen")
    }

    missingType match {
      case (colName :: tail, dataType) =>
        dataframe.withColumn(
          colName,
          buildMissingColumn(List(colName), tail, dataType)
        )
      case (_, _) => dataframe
    }
  }

  /** Perform an union between two dataframe. Fixes any missing column from the originalDF by adding
    * null values where needed and also fixes any missing column in the incoming DF (see {@link
    * computeCompatibleSchema})
    */
  private def computeDataframeUnion(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {
    val patchedExistingDF = addMissingAttributes(existingDF, incomingDF)
    val patchedIncomingDF = addMissingAttributes(incomingDF, existingDF)

    patchedIncomingDF.unionByName(patchedExistingDF)
  }

  private def addMissingAttributes(existingDF: DataFrame, incomingDF: DataFrame) = {
    val missingTypesInExisting = findMissingColumnsType(existingDF.schema, incomingDF.schema)
    val patchedExistingDF = missingTypesInExisting
      .fold(existingDF) { missingTypes =>
        missingTypes.foldLeft(existingDF) {
          buildMissingType
        }
      }
    logger.info(patchedExistingDF.schemaString())
    patchedExistingDF
  }
}
