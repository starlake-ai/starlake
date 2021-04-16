package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.utils.DataTypeEx._
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame}

sealed case class MetricsTable(value: String) {
  override def toString: String = value
}

object MetricsTable {

  def fromString(value: String): MetricsTable = {
    value.toUpperCase match {
      case "continuous"  => MetricsTable.CONTINUOUS
      case "discrete"    => MetricsTable.DISCRETE
      case "frequencies" => MetricsTable.FREQUENCIES
    }
  }

  object CONTINUOUS extends MetricsTable("continuous")

  object DISCRETE extends MetricsTable("discrete")

  object FREQUENCIES extends MetricsTable("frequencies")

  val metrics: Set[MetricsTable] = Set(CONTINUOUS, DISCRETE, FREQUENCIES)
}

object Metrics extends StrictLogging {

  case class MetricsDatasets(
    continuousDF: Option[DataFrame],
    discreteDF: Option[DataFrame],
    frequenciesDF: Option[DataFrame]
  )

  /** Case class ContinuousMetric with all corresponding Metrics
    *
    * @param name     : the name of the variable
    * @param function : the metric function
    */
  case class ContinuousMetric(name: String, function: Column => Column)

  object Min extends ContinuousMetric("min", min)

  object Max extends ContinuousMetric("max", max)

//  object Count extends ContinuousMetric("count", count)

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
    */

  val continuousMetrics: List[ContinuousMetric] = List(
    Min,
    Max,
    Mean,
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
        || e === "",
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
    val listColumns = metricFrame.columns.filter(_.contains(s"($nameCol)")).sorted
    //Select only columns in listColumns
    val selectedListColumns: DataFrame = metricFrame.select(listColumns.head, listColumns.tail: _*)
    //Reduce decimal values to 3
    val broundColumns: DataFrame = selectedListColumns.select(
      selectedListColumns.columns.map(c => bround(col(c), 3).alias(c)): _*
    )
    //Add column Variables that contains the nameCol
    val addVariablesColumn: DataFrame = broundColumns.withColumn("attribute", lit(nameCol))
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

    val datasetAttributes =
      dataset.schema.fields.filter(_.dataType.isOfValidContinuousType).map(_.name).toList
    logger.info(s"Valid Continuous datasetAttributes Attrs =$datasetAttributes")
    val intersectionAttributes = datasetAttributes.intersect(continuousAttributes)
    logger.info(s"Valid intersectionAttributes Attrs =$intersectionAttributes")
    if (intersectionAttributes.nonEmpty) {
      intersectionAttributes
    } else {
      Nil
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
  ): Option[DataFrame] = {
    val attributeChecked = extractMetricsAttributes(dataset, continuousAttributes)
    logger.info(s"Continuous attributes : $attributeChecked")
    val colRenamed: List[String] = "attribute" :: operations.map(_.name)
    val metrics: List[Column] =
      attributeChecked.flatMap(name => operations.map(metric => metric.function(col(name))))
    val dropAttributes =
      dataset.schema.fields.map(_.name).toList.filterNot(continuousAttributes.toSet)

    metrics match {
      case Nil =>
        None
      case _ =>
        val droppedDataset = dataset.drop(dropAttributes: _*)
        val metricFrame: DataFrame = droppedDataset.agg(metrics.head, metrics.tail: _*)
        val matrixMetric =
          attributeChecked
            .map(x => regroupContinuousMetricsByVariable(x, metricFrame))
            .reduce(_.union(_))

        val res = matrixMetric
          .select(colRenamed.head, colRenamed.tail: _*)
          .withColumn("cometMetric", lit("Continuous"))
        Some(res)
    }
  }

  case class DiscreteMetric(name: String, function: ((Column, DataFrame)) => Column)

  object CountDistinct extends DiscreteMetric("countDistinct", customCountDistinct)

  object CatCountFreq extends DiscreteMetric("catCountFreq", customCatCountFreq)

  object CountDiscrete extends DiscreteMetric("countByCategory", customCountDiscrete)

  object CountMissValuesDiscrete
      extends DiscreteMetric("missingValuesDiscrete", customCountMissValuesDiscrete)

  /** List of all available metrics.
    */

  val discreteMetrics: List[DiscreteMetric] =
    List(CountDistinct, CatCountFreq, CountMissValuesDiscrete)

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
    val (colVar, dataCatCountFreq) = colNamDataCatCountFreq
    val listCol: List[Column] =
      operations.map(metric => metric.function((colVar, dataCatCountFreq)))

    dataCatCountFreq
      .withColumn(
        "cat_count_freq",
        struct(
          col("Category").cast(StringType).as("category"),
          col("CountDiscrete").cast(LongType).as("countDiscrete"),
          col("Frequencies").cast(DoubleType).as("frequency")
        )
      )
      .agg(listCol.head, listCol.tail: _*)
      .withColumn("attribute", lit(colVar.toString()))

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
          col("Category").isNotNull,
          lit(0)
        ).otherwise(col("CountDiscrete"))
      )
      .withColumn(
        "CategoryChange",
        col("Category")
      )

    (e, dataCatCountFreq)

  }

  /** Function to extract the column that contains the list of category
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

  def metricFrequency(dataCategoryCount: DataFrame): Column = {
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

  /** Function to extract the column that contains the list of struct cat_count_freq
    *
    * @param dataCategoryCount
    * @return
    */
  def metricCatCountFreq(dataCategoryCount: DataFrame): Column = {
    collect_list(col("cat_count_freq")).as("catCountFreq")
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
    * @param e                 : name of the column
    * @param dataCategoryCount : the dataframe obtain from categoryCountFreqDataframe()
    * @param metricName        : te metric name
    * @param metricFunction    : the metric function
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
    val (colVar, dataCatCount) = colNameDataCatCount
    customMetricDiscret(
      colVar,
      dataCatCount,
      "countDistinct",
      metricCountDistinct
    )
  }

  /** Customize catCountFreq for discrete variable
    *
    * @param colNameDataCatCount
    * @return
    */
  def customCatCountFreq(colNameDataCatCount: (Column, DataFrame)): Column = {
    customMetricDiscret(
      colNameDataCatCount._1,
      colNameDataCatCount._2,
      "catCountFreq",
      metricCatCountFreq
    )
  }

  /** Customize Count Discrete for discrete variable
    *
    * @param colNameDataCatCount : couple of name of the variable and the dataframe obtain from categoryCountFreqDataframe()
    * @return Column : the computed value of the function metricCountDiscret
    */

  def customCountDiscrete(colNameDataCatCount: (Column, DataFrame)): Column = {
    val (colVar, dataCatCount) = colNameDataCatCount
    customMetricDiscret(
      colVar,
      dataCatCount,
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
      metricFrequency
    )
  }

  /** Customize number of Missing Values for discrete variable
    *
    * @param colNameDataCatCount : couple of name of the variable and the dataframe obtain from categoryCountFreqDataframe()
    * @return Column : the computed value of the function metricMissingValues
    */

  def customCountMissValuesDiscrete(colNameDataCatCount: (Column, DataFrame)): Column = {
    val (colVar, dataCatCount) = colNameDataCatCount
    customMetricDiscret(
      colVar,
      dataCatCount,
      "missingValuesDiscrete",
      metricMissingValues
    )
  }

  /** Function to compute and to combine all the partial DataFrame metric by variable (to get one DataFrame by row).
    *
    * @param dataInit   : initial DataFrame.
    * @param discreteAttrs : name of the variable.
    * @param operations : list of metrics you want to calculate.
    * @return DataFrame : DataFrame with alle the metric by variable by row
    */

  def computeDiscretMetric(
    dataInit: DataFrame,
    discreteAttrs: List[String],
    operations: List[DiscreteMetric]
  ): Option[DataFrame] = {

    val headerDataUse =
      dataInit.schema.fields.filter(_.dataType.isOfValidDiscreteType()).map(_.name).toList
    logger.info("Discrete Headers -> " + headerDataUse.mkString(","))
    val intersectionHeaderAttributes = headerDataUse.intersect(discreteAttrs)
    logger.info(
      "intersectionHeaderAttributes Headers -> " + intersectionHeaderAttributes.mkString(",")
    )

    val dropAttributes = dataInit.schema.fields.map(_.name).filterNot(discreteAttrs.toSet)

    val attributeChecked: List[String] =
      if (intersectionHeaderAttributes.nonEmpty) {
        intersectionHeaderAttributes
      } else {
        Nil
      }

    logger.info("attributeChecked Headers -> " + attributeChecked.mkString(","))
    attributeChecked match {
      case Nil =>
        None
      case _ =>
        val dataset = dataInit.drop(dropAttributes: _*)
        val colRenamed: List[String] = "attribute" :: operations.map(_.name)
        val matrixMetric: DataFrame = attributeChecked
          .map(name => categoryCountFreqDataframe(col(name), dataset))
          .map(colData => dataToMetricData(colData, operations))
          .reduce(_.union(_))
        val res = matrixMetric
          .select(colRenamed.head, colRenamed.tail: _*)
          .withColumn("cometMetric", lit("Discrete"))
        Some(res)
    }

  }

}
