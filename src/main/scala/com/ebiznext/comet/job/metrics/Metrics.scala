package com.ebiznext.comet.job.metrics

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions.{callUDF, lit, _}
import org.apache.spark.sql.{Column, DataFrame}

object Metrics extends StrictLogging {

  /** Case class ContinuousMetric with all corresponding Metrics
    *
    * @param name     : the name of the variable
    * @param function : the metric function
    */
  case class ContinuousMetric(name: String, function: Column => Column)

  object Min extends ContinuousMetric("Min", min)

  object Max extends ContinuousMetric("Max", max)

  object Count extends ContinuousMetric("Count", count)

  object Sum extends ContinuousMetric("Sum", sum)

  object Skewness extends ContinuousMetric("Skewness", skewness)

  object Kurtosis extends ContinuousMetric("Kurtosis", kurtosis)

  object Percentile75 extends ContinuousMetric("Percentile75", percentile75)

  object Percentile25 extends ContinuousMetric("Percentile25", percentile25)

  object Median extends ContinuousMetric("Median", customMedian)

  object Mean extends ContinuousMetric("Mean", customMean)

  object Variance extends ContinuousMetric("Var", customVariance)

  object Stddev extends ContinuousMetric("Stddev", customStddev)

  object CountMissValues extends ContinuousMetric("CountMissValues", customCountMissValues)

  /** List of all available metrics
    *
    */

  val continuousMetrics: List[ContinuousMetric] = List(
    Min,
    Max,
    Mean,
    Count,
    CountMissValues,
    Variance,
    Stddev,
    Sum,
    Skewness,
    Kurtosis,
    Percentile25,
    Median,
    Percentile75
  )

  /** Customize function metric in the case continuous variabes used for :  mean, variance and stddev
    *
    * @param e              : the column
    * @param metricName     : the name of the metric
    * @param metricFunction : the metric function
    * @return : the computed value of the function
    */

  def customMetric(e: Column, metricName: String, metricFunction: Column => Column): Column = {
    val aliasMetricName: String = metricName + "(" + e.toString() + ")"
    metricFunction(e).as(aliasMetricName)
  }

  /** Customize mean of the column e
    *
    * @param e : the column
    * @return Integer : the computed  value of the mean
    */

  def customMean(e: Column): Column = {
    customMetric(e: Column, "Mean", mean)
  }

  /** Customize variance of the column e
    *
    * @param e : the name of the column
    * @return Integer : the computed value of the variance
    */

  def customVariance(e: Column): Column = {
    customMetric(e: Column, "Var", variance)
  }

  /** Customize Stddev of the column e
    *
    * @param e : the name of the column
    * @return Integer : the computed value of the Stddev
    */

  def customStddev(e: Column): Column = {
    customMetric(e: Column, "Stddev", stddev)
  }

  /** Customize function metric in the case continuous variabes used for : percentile 25, median and percentile75
    *
    * @param e              : the column
    * @param metricName     : the name of the metric
    * @param metricFunction : the metric function
    * @param approxMethod   : the approximation method
    * @param approxValue    : the value to pass to stat_method
    * @return
    */
  def customMetricUDF(
    e: Column,
    metricName: String,
    metricFunction: (String, Column*) => Column,
    approxMethod: String,
    approxValue: Double
  ): Column = {

    val aliasMetric: String = metricName + "(" + e.toString() + ")"
    metricFunction(approxMethod, e, lit(approxValue)).as(aliasMetric)
  }

  /** Customize percentile of order 0.25 of the column e
    *
    * @param e : the column
    * @return Integer : the computed value of the percentile of order 0.25
    */

  def percentile25(e: Column): Column = {
    customMetricUDF(e: Column, "Percentile25", callUDF, "percentile_approx", 0.25)
  }

  /** Customize Median of the column e
    *
    * @param e : the column
    * @return Integer : the computed value of the Median
    */

  def customMedian(e: Column): Column = {
    customMetricUDF(e: Column, "Median", callUDF, "percentile_approx", 0.50)
  }

  /** Customize percentile of order 0.75 of the column e
    *
    * @param e : the column
    * @return Integer : the computed value of the percentile of order 0.75
    */

  def percentile75(e: Column): Column = {
    customMetricUDF(e: Column, "Percentile75", callUDF, "percentile_approx", 0.75)
  }

  /** Customize missing values
    *
    * @param e : the column
    * @return Integer : the number of missing values, NaN  values and null values
    */
  def customCountMissValues(e: Column): Column = {
    val nameCol = e.toString()
    val aliasCountMissValues: String = "CountMissValues" + "(" + nameCol + ")"
    val unionMissingValues = sum(
      when(
        e.isNull
        || e === ""
        || e === " "
        || e.isNaN,
        1
      ).otherwise(0)
    )
    unionMissingValues.as(aliasCountMissValues)
  }

  /** Function to regroup and reformat all metrics for a given variable
    *
    * @param nameCol     : the name of the column.
    * @param metricFrame : the DataFrame of all the computed metrics for each variable by columns.
    * @return : the DataFrame metric  associated to the variable (namecol).
    */

  def regroupContinuousMetricsByVariable(nameCol: String, metricFrame: DataFrame): DataFrame = {
    //Get the whole list of headers that contains the column name
    val listColumns = metricFrame.columns.filter(_.contains(nameCol)).sorted
    //Select only columns in listColumns
    val selectedListColumns: DataFrame = metricFrame.select(listColumns.head, listColumns.tail: _*)
    //Reduce decimal values to 3
    val broundColumns: DataFrame = selectedListColumns.select(
      selectedListColumns.columns.map(c => bround(col(c), 3).alias(c)): _*
    )
    //Add column Variables that contains the nameCol
    val addVariablesColumn: DataFrame = broundColumns.withColumn("Variables", lit(nameCol))
    //Remove nameCol to keep only the metric name foreach metric.
    val removeNameColumnMetric = addVariablesColumn.columns.toList
      .map(str => str.replaceAll("\\(" + nameCol + "\\)", ""))
      .map(_.capitalize)
    addVariablesColumn.toDF(removeNameColumnMetric: _*)
  }

  private def extractMetricsAttributes(
    dataset: DataFrame,
    continuousAttributes: List[String]
  ): List[String] = {
    println("============================")
    dataset.dtypes.foreach(println)
    val datasetAttributes = dataset.dtypes.flatMap {
      case (f, t) =>
        if (t.startsWith("StructType") || t.startsWith("ArrayType"))
          None
        else
          Some(f)
    } toList

    datasetAttributes.foreach(println)

    println("============================")
    val intersectionAttributes = datasetAttributes.intersect(continuousAttributes)
    val listDifference = continuousAttributes.filterNot(datasetAttributes.contains)
    if (intersectionAttributes.nonEmpty) {
      intersectionAttributes
    } else {
      logger.warn(
        "These attributes are not part of the variable names: " + listDifference.mkString(",")
      )
      datasetAttributes
    }
  }

  /** Function to compute the DataFrame metrics by row
    *
    * @param dataset              : initial DataFrame.
    * @param continuousAttributes : name list of all variables.
    * @param operations           : list of metrics you want to calculate.
    * @return DataFrame : DataFrame metric  of all variables by row.
    */

  def computeContinuousMetric(
    dataset: DataFrame,
    continuousAttributes: List[String],
    operations: List[ContinuousMetric]
  ): DataFrame = {
    val attributeChecked = extractMetricsAttributes(dataset, continuousAttributes)
    val colRenamed: List[String] = "Variables" :: operations.map(_.name)
    val metrics: List[Column] =
      attributeChecked.flatMap(name => operations.map(metric => metric.function(col(name))))
    val metricFrame: DataFrame = dataset.agg(metrics.head, metrics.tail: _*)
    val matrixMetric =
      attributeChecked
        .map(x => regroupContinuousMetricsByVariable(x, metricFrame))
        .reduce(_.union(_))
    matrixMetric
      .select(colRenamed.head, colRenamed.tail: _*)
      .withColumn("_metricType_", lit("Continuous"))

  }

  case class DiscreteMetric(name: String, function: (DataFrame, String) => DataFrame)

  object Category extends DiscreteMetric("Category", customCategory)

  object CountDiscrete extends DiscreteMetric("CountDiscrete", customCountDiscrete)

  object Frequencies extends DiscreteMetric("Frequencies", customFrequencies)

  object CountMissValuesDiscrete
      extends DiscreteMetric("CountMissValuesDiscrete", customCountMissValuesDiscrete)

  /** List of all available metrics.
    *
    */

  val discreteMetrics: List[DiscreteMetric] =
    List(Category, CountDiscrete, Frequencies, CountMissValuesDiscrete)

  /** Customize count for discrete variable
    *
    * @param dataInit : initial DataFrame
    * @param e        : name of the column
    * @return DataFrame : with the name of category and the value of the count
    */
  def customCountDiscrete(dataInit: DataFrame, e: String): DataFrame = {
    val valueCountByColumn: DataFrame = dataInit.groupBy(e).count()
    valueCountByColumn.toDF("Category", "CountDiscrete")
  }

  /** Customize Category for discrete variable
    *
    * @param dataInit : initial DataFrame
    * @param e        : name of the column
    * @return DataFrame : with the name of category
    */

  def customCategory(dataInit: DataFrame, e: String): DataFrame = {
    val valueCountByColumn: DataFrame = customCountDiscrete(dataInit, e)
    valueCountByColumn.select("Category")
  }

  /** Customize Frequencies for discrete variable
    *
    * @param dataInit : initial DataFrame
    * @param e        : name of the column
    * @return DataFrame : with the name of category and the values of the Frequencies
    */

  def customFrequencies(dataInit: DataFrame, e: String): DataFrame = {
    val valueCountByColumn: DataFrame = customCountDiscrete(dataInit, e)
    val sumValues: Long = valueCountByColumn.agg(sum("CountDiscrete")).first.getAs[Long](0)
    val valueFrequencies: DataFrame =
      valueCountByColumn.withColumn(
        "Frequencies",
        bround(valueCountByColumn("CountDiscrete") / sumValues, 3)
      )
    valueFrequencies.select("Category", "Frequencies")
  }

  /** Customize Frequencies for discrete variable
    *
    * @param dataInit : initial DataFrame
    * @param e        : the name of the column
    * @return DataFrame : with the sum of missing values, NaN  values and null values
    */
  def customCountMissValuesDiscrete(dataInit: DataFrame, e: String): DataFrame = {
    val numMissValues =
      dataInit.filter(dataInit(e).isNull || dataInit(e) === " " || dataInit(e).isNaN).count()
    val subColFrame: DataFrame =
      customCategory(dataInit, e).withColumn("CountMissValuesDiscrete", lit(numMissValues))
    subColFrame.toDF("Category", "CountMissValuesDiscrete")
  }

  /** Function to regroup each partial DataFrame metric by variable into one joined by Category
    *
    * @param dataInit   : initial DataFrame.
    * @param name       : name of the variable.
    * @param operations : list of metrics you want to calculate.
    * @return DataFrame : partial DataFrame metric by variables (name).
    */

  def regroupDiscreteMetricsByVariable(
    dataInit: DataFrame,
    name: String,
    operations: List[DiscreteMetric]
  ): DataFrame = {
    val metrics: List[DataFrame] = operations.map(metric => metric.function(dataInit, name))
    val metricFrame: DataFrame = metrics.reduce((a, b) => a.join(b, "Category"))
    metricFrame.withColumn("Variables", lit(name))
  }

  /** Function to combine all the partial DataFrame metric by variable (to get one DataFrame by row).
    *
    * @param dataset            : initial DataFrame.
    * @param discreteAttributes : name of the variable.
    * @param operations         : list of metrics you want to calculate.
    * @return DataFrame : DataFrame with alle the metric by variable by row
    */

  def computeDiscretMetric(
    dataset: DataFrame,
    discreteAttributes: List[String],
    operations: List[DiscreteMetric]
  ): DataFrame = {
    val attributeChecked = extractMetricsAttributes(dataset, discreteAttributes)
    val colRenamed: List[String] = "Variables" :: operations.map(_.name)
    val matrixMetric =
      attributeChecked
        .map(x => regroupDiscreteMetricsByVariable(dataset, x, operations))
        .reduce(_.union(_))
    matrixMetric
      .select(colRenamed.head, colRenamed.tail: _*)
      .withColumn("_metricType_", lit("Discrete"))
  }

}
