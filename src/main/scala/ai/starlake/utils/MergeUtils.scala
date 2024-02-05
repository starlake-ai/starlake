package ai.starlake.utils

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, DatasetLogging}

object MergeUtils extends StrictLogging with DatasetLogging {

  /** Compute a new schema that is compatible with merge operations. Built recursively from the
    * incoming schema to retain the latest attributes, but without the columns that does not exist
    * yet. Ensures that the incomingSchema contains all the columns from the existingSchema.
    */
  def computeCompatibleSchema(
    existingSchema: StructType,
    incomingSchema: StructType
  ): StructType = {
    val actualColumns = existingSchema.map(field => field.name.toLowerCase() -> field).toMap
    // computeNewColumns(actualSchema, expectedSchema)

    StructType(
      incomingSchema
        .flatMap(incomingField =>
          actualColumns
            .get(incomingField.name.toLowerCase())
            .map(existingField =>
              (existingField.dataType, incomingField.dataType) match {
                case (existingType: StructType, incomingType: StructType) =>
                  incomingField.copy(dataType = computeCompatibleSchema(existingType, incomingType))
                case (
                      ArrayType(existingType: StructType, _),
                      ArrayType(incomingType: StructType, nullable)
                    ) =>
                  incomingField
                    .copy(dataType =
                      ArrayType(computeCompatibleSchema(existingType, incomingType), nullable)
                    )
                case (_, _) => incomingField
              }
            )
        )
    )
  }
  def computeNewColumns(
    existingSchema: StructType,
    incomingSchema: StructType
  ): List[StructField] = {
    val actualColumns = existingSchema.map(field => field.name.toLowerCase() -> field).toMap
    val expectedColumns = incomingSchema.map(field => field.name.toLowerCase() -> field).toMap

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
    expectedColumns.filter { case (key, value) => newColumns.contains(key) }.values.toList
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

}
