package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.job.metrics.Metrics._
import org.apache.spark.sql.functions._

class StatDescJobSpec extends TestHelper {

  /**
    * Inputs for the test :  Header (list of the variable) and Metrics (Metrics to use)
    */

  val listContnuousAttributes: List[String] =
    Seq("SepalLength", "SepalWidth", "PetalLength", "PetalWidth").toList
  val listDiscreteAttributes: List[String] = Seq("Name").toList

  val partialContinuousMetric: List[ContinuousMetric] = List(Min, Max)

  /**
    * Read the data .csv
    */

  val dataInitialUsed = sparkSession.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .option("inferSchema", "true")
    .load("./src/test/resources/iris.csv")

  /**
    * Descriptive statistics of the dataframe for Quantitative variable:
    */
  dataInitialUsed.printSchema()

  val result0 = computeContinuousMetric(
    dataInitialUsed,
    listContnuousAttributes,
    continuousMetrics
  )

  val result1 = computeContinuousMetric(
    dataInitialUsed,
    listContnuousAttributes,
    partialContinuousMetric
  )

  /**
    * 1- test : Test on the mean of the dimension
    */
  val dimensionTable = (partialContinuousMetric.size + 1) * (listContnuousAttributes.size + 1)

  logger.info(s"-->$dimensionTable")

  val dimensionDataframe = result1.map { result1 =>
    result1.printSchema()
    (result1.columns.size - 1) * (result1
      .select(col("attribute"))
      .collect()
      .map(_.getString(0))
      .toList
      .size + 1)
  }

  "The size  of the Table " should "be tested" in {
    assert(dimensionTable - dimensionDataframe.getOrElse(0) == 0)
  }

  /**
    * 2- test : Test for all values of the Mean
    */

  val meanList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(avg(name)).first().getDouble(0))

  val meanListTable: List[Double] = result0.map { result0 =>
    result0.select(col("mean")).collect().map(_.getDouble(0)).toList
  } getOrElse (Nil)

  "All values of The Mean " should "be tested" in {
    assert(meanList.zip(meanListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    * 3- test : Test for all values of the Min
    */

  val minList: List[Double] = listContnuousAttributes.map(
    name => dataInitialUsed.select(min(name)).first().getDouble(0)
  )

  val minListTable: List[Double] = result0.map { result0 =>
    result0.select(col("min")).collect().map(_.getDouble(0)).toList
  } getOrElse (Nil)

  "All values of The Min" should "be tested" in {
    assert(minList.zip(minListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    * 4- test : Test for all values of the Max
    */

  val maxList: List[Double] = listContnuousAttributes.map(
    name => dataInitialUsed.select(max(name)).first().getDouble(0)
  )

  val maxListTable: List[Double] = result0.map { result0 =>
    result0.select(col("max")).collect().map(_.getDouble(0)).toList
  } getOrElse (Nil)

  "All values of The Max" should "be tested" in {
    assert(maxList.zip(maxListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    * 5- test : Test for all values of the standardDev
    */

  val stddevList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(stddev(name)).first().getDouble(0))

  val stddevListTable: List[Double] = result0.map { result0 =>
    result0.select(col("standardDev")).collect().map(_.getDouble(0)).toList
  } getOrElse Nil

  "All values of The standardDev" should "be tested" in {
    assert(stddevList.zip(stddevListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /**
    * 6- test : Test for all values of the Skewness
    */

  val skewnessList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(skewness(name)).first().getDouble(0))

  val skewnessListTable: List[Double] = result0.map { result0 =>
    result0.select(col("skewness")).collect().map(_.getDouble(0)).toList
  } getOrElse (Nil)
  "All values of The Skewness" should "be tested" in {
    assert(skewnessList.zip(skewnessListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /**
    * 7- test : Test for all values of the kurtosis
    */

  val kurtosisList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(kurtosis(name)).first().getDouble(0))

  val kurtosisListTable: List[Double] = result0.map { result0 =>
    result0.select(col("kurtosis")).collect().map(_.getDouble(0)).toList
  } getOrElse (Nil)

  "All values of The Kurtosis" should "be tested" in {
    assert(kurtosisList.zip(kurtosisListTable).map(x => x._1 - x._2).sum <= 0.001)
  }
}
