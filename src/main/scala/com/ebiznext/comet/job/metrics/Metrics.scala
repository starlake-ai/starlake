package com.ebiznext.comet.job.metrics

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Metrics extends StrictLogging {

  /** Case class ContinuousMetric with all corresponding Metrics
    *
    * @param name     : the name of the variable
    * @param function : the metric function
    */
  case class ContinuousMetric(name: String, function: Column => Column)

  object Min extends ContinuousMetric("min", min)

  object Max extends ContinuousMetric("max", max)

  object Count extends ContinuousMetric("count", count)

  object Sum extends ContinuousMetric("sum", sum)

  object Skewness extends ContinuousMetric("skewness", skewness)

  object Kurtosis extends ContinuousMetric("kurtosis", kurtosis)

  object Percentile75 extends ContinuousMetric("percentile75", percentile75)

  object Percentile25 extends ContinuousMetric("percentile25", percentile25)

  object Median extends ContinuousMetric("median", customMedian)

  object Mean extends ContinuousMetric("mean", customMean)

  object Variance extends ContinuousMetric("variance", customVariance)

  object Stddev extends ContinuousMetric("standardDev", customStddev)

  object CountMissValues extends ContinuousMetric("missingValues", customCountMissValues)

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
    customMetric(e: Column, "mean", mean)
  }

  /** Customize variance of the column e
    *
    * @param e : the name of the column
    * @return Integer : the computed value of the variance
    */

  def customVariance(e: Column): Column = {
    customMetric(e: Column, "variance", variance)
  }

  /** Customize Stddev of the column e
    *
    * @param e : the name of the column
    * @return Integer : the computed value of the Stddev
    */

  def customStddev(e: Column): Column = {
    customMetric(e: Column, "standardDev", stddev)
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
    customMetricUDF(e: Column, "percentile25", callUDF, "percentile_approx", 0.25)
  }

  /** Customize Median of the column e
    *
    * @param e : the column
    * @return Integer : the computed value of the Median
    */

  def customMedian(e: Column): Column = {
    customMetricUDF(e: Column, "median", callUDF, "percentile_approx", 0.50)
  }

  /** Customize percentile of order 0.75 of the column e
    *
    * @param e : the column
    * @return Integer : the computed value of the percentile of order 0.75
    */

  def percentile75(e: Column): Column = {
    customMetricUDF(e: Column, "percentile75", callUDF, "percentile_approx", 0.75)
  }

  /** Customize missing values
    *
    * @param e : the column
    * @return Integer : the number of missing values, NaN  values and null values
    */
  def customCountMissValues(e: Column): Column = {
    val nameCol = e.toString()
    val aliasCountMissValues: String = "missingValues" + "(" + nameCol + ")"
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
    val addVariablesColumn: DataFrame = broundColumns.withColumn("variableName", lit(nameCol))
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
    val colRenamed: List[String] = "variableName" :: operations.map(_.name)
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

  case class DiscreteMetric(name: String, function: ((Column, DataFrame)) => Column)

  object Category extends DiscreteMetric("category", customCategory)

  object CountDistinct extends DiscreteMetric("countDistinct", customCountDistinct)

  object CountDiscrete extends DiscreteMetric("countByCategory", customCountDiscrete)

  object Frequencies extends DiscreteMetric("frequencies", customFrequencies)

  object CountMissValuesDiscrete
      extends DiscreteMetric("missingValuesDiscrete", customCountMissValuesDiscrete)

  /** List of all available metrics.
    *
    */

  val discreteMetrics: List[DiscreteMetric] =
    List(Category, CountDistinct, CountDiscrete, Frequencies, CountMissValuesDiscrete)

  /** Function to compute the Dataframe metric by variable
    *
    * @param colNamDataCatCountFreq : tuple of column variable and the Dataframe with Category, Count and Frequencies obtain from  categoryCountFreqDataframe()
    * @param operations             : list of metrics you want to calculate.
    * @return Dataframe             : with all the values of discrete metrics
    */

  def dataToMetricData(
    colNamDataCatCountFreq: (Column, DataFrame),
    operations: List[DiscreteMetric]
  ): DataFrame = {
    val colVar: Column = colNamDataCatCountFreq._1
    val dataCatCountFreq: DataFrame = colNamDataCatCountFreq._2
    val listcol: List[Column] =
      operations.map(metric => metric.function((colVar, dataCatCountFreq)))
    dataCatCountFreq
      .agg(listcol.head, listcol.tail: _*)
      .withColumn("variableName", lit(colVar.toString()))
  }

  /** Function to compute the Dataframe with Category, Count and Frequencies obtain from the initial Dataframe
    *
    * @param e        : column of the variable.
    * @param dataInit : initial DataFrame.
    * @return (Column, DataFrame) : tuple2 of the column of the variable and the initial Dataframe
    */

  def categoryCountFreqDataframe(e: Column, dataInit: DataFrame): (Column, DataFrame) = {
    val dataCatCount: DataFrame = dataInit.groupBy(e).count().toDF("Category", "CountDiscrete")
    val sumValues: Long = dataInit.count()

    val dataCatCountFreq: DataFrame = dataCatCount
      .withColumn("Frequencies", bround(dataCatCount("CountDiscrete") / sumValues, 3))
      .withColumn(
        "NumMissVals",
        when(
          col("Category").isNotNull.&&(col("Category").notEqual(" ")).&&(!col("Category").isNaN),
          lit(0)
        ).otherwise(col("CountDiscrete"))
      )
      .withColumn(
        "CategoryChange",
        when(col("Category").isNull, "Null_Values")
          .when(col("Category").isNaN, "NaN_Values")
          .when(col("Category").equalTo(" "), "Empty_Values")
          .otherwise(col("Category"))
      )

    (e, dataCatCountFreq)

  }

  /**  Function to extract the column that contains the list of category
    *
    * @param dataCategoryCount : the data frame obtain from categoryCountFreqDataframe()
    * @return Column : of that contain the list of category values
    */

  def metricCategory(dataCategoryCount: DataFrame): Column = {
    collect_list(col("Category"))
  }

  /** Function to extract the column that contains the list of CountDiscret
    *
    * @param dataCategoryCount : the data frame obtain from categoryCountFreqDataframe()
    * @return Column : of that contain the list of CountDiscrete values
    */

  def metricCountDiscret(dataCategoryCount: DataFrame): Column = {
    collect_list(map(col("CategoryChange"), col("CountDiscrete"))).as("countByCategory")
  }

  /** Function to extract the column that contains the list of frequencies
    *
    * @param dataCategoryCount : the data frame obtain from categoryCountFreqDataframe()
    * @return Column : of that contain the list of frequencies values
    */

  def metricFrenquence(dataCategoryCount: DataFrame): Column = {
    collect_list(map(col("CategoryChange"), col("Frequencies"))).as("frequencies")
  }

  /** Function to extract the column that contains the list of CountDistinct
    *
    * @param dataCategoryCount : the data frame obtain from categoryCountFreqDataframe()
    * @return Column : of that contain the list of CountDistinct values
    */

  def metricCountDistinct(dataCategoryCount: DataFrame): Column = {
    count(col("Category"))
  }

  /** Function to extract the column that contains the list of number of Missing values
    *
    * @param dataCategoryCount : the data frame obtain from categoryCountFreqDataframe()
    * @return Column : of that contain the list of Missing Values values
    */

  def metricMissingValues(dataCategoryCount: DataFrame): Column = {
    sum(col("NumMissVals"))
  }

  /** Customize Metric Discret for discrete variable
    *
    * @param e  : name of the column
    * @param dataCategoryCount : the dataframe obtain from categoryCountFreqDataframe()
    * @param metricName     : te metric name
    * @param metricFunction : the metric function
    * @return Column : the computed value of the function
    */

  def customMetricDiscret(
    e: Column,
    dataCategoryCount: DataFrame,
    metricName: String,
    metricFunction: (DataFrame) => Column
  ): Column = {
    val aliasMetricName: String = metricName
    metricFunction(dataCategoryCount).as(aliasMetricName)
  }

  /** Customize Category for discrete variable
    *
    * @param colNameDataCatCount : couple of name of the variable and the dataframe obtain from categoryCountFreqDataframe()
    * @return Column : the computed value of the function metricCategory
    */

  def customCategory(colNameDataCatCount: (Column, DataFrame)): Column = {
    customMetricDiscret(colNameDataCatCount._1, colNameDataCatCount._2, "category", metricCategory)
  }

  /** Customize CountDistinct for discrete variable
    *
    * @param colNameDataCatCount : couple of name of the variable and the dataframe obtain from categoryCountFreqDataframe()
    * @return Column : the computed value of the function metricCountDistinct
    */

  def customCountDistinct(colNameDataCatCount: (Column, DataFrame)): Column = {
    customMetricDiscret(
      colNameDataCatCount._1,
      colNameDataCatCount._2,
      "countDistinct",
      metricCountDistinct
    )
  }

  /** Customize Count Discrete for discrete variable
    *
    * @param colNameDataCatCount : couple of name of the variable and the dataframe obtain from categoryCountFreqDataframe()
    * @return Column : the computed value of the function metricCountDiscret
    */

  def customCountDiscrete(colNameDataCatCount: (Column, DataFrame)): Column = {
    customMetricDiscret(
      colNameDataCatCount._1,
      colNameDataCatCount._2,
      "countByCategory",
      metricCountDiscret
    )
  }

  /** Customize Count Distinct for discrete variable
    *
    * @param colNameDataCatCount : couple of name of the variable and the dataframe obtain from categoryCountFreqDataframe()
    * @return Column : the computed value of the function metricCountDistinct
    */

  def customFrequencies(colNameDataCatCount: (Column, DataFrame)): Column = {
    customMetricDiscret(
      colNameDataCatCount._1,
      colNameDataCatCount._2,
      "frequencies",
      metricFrenquence
    )
  }

  /** Customize number of Missing Values for discrete variable
    *
    * @param colNameDataCatCount : couple of name of the variable and the dataframe obtain from categoryCountFreqDataframe()
    * @return Column : the computed value of the function metricMissingValues
    */

  def customCountMissValuesDiscrete(colNameDataCatCount: (Column, DataFrame)): Column = {
    customMetricDiscret(
      colNameDataCatCount._1,
      colNameDataCatCount._2,
      "missingValuesDiscrete",
      metricMissingValues
    )
  }

  /** Function to compute and to combine all the partial DataFrame metric by variable (to get one DataFrame by row).
    *
    * @param dataInit   : initial DataFrame.
    * @param attributes : name of the variable.
    * @param operations : list of metrics you want to calculate.
    * @param threshold  : number max  for the number of categories
    * @return DataFrame : DataFrame with alle the metric by variable by row
    */

  def computeDiscretMetric(
    dataInit: DataFrame,
    attributes: List[String],
    operations: List[DiscreteMetric]
  ): DataFrame = {

    val headerDataUse = dataInit.columns.toList
    val intersectionHeaderAttributes = headerDataUse.intersect(attributes)
    val listDifference = attributes.filterNot(headerDataUse.contains)

    val attributeChecked = intersectionHeaderAttributes.nonEmpty match {
      case true => attributes
      case false =>
        logger.error(
          "These attributes are not part of the variable names: " + listDifference.mkString(",")
        )
        headerDataUse
    }

    val colRenamed: List[String] = "variableName" :: operations.map(_.name)
    val matrixMetric: DataFrame = attributeChecked
      .map(name => categoryCountFreqDataframe(col(name), dataInit))
      .map(colData => dataToMetricData(colData, operations))
      .reduce(_.union(_))
    matrixMetric
      .select(colRenamed.head, colRenamed.tail: _*)
      .withColumn("_metricType_", lit("Discrete"))

  }
}
