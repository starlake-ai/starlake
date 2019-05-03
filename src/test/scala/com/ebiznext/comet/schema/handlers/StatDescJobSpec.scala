package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.job.metrics.Metrics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec

import scala.reflect.runtime.universe._

class StatDescJobSpec extends FlatSpec {

  /**
    * Custom Log Levels
    */
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

  /**
    * Spark configuration
    */

  val conf = new SparkConf() // set the configuration
    .setAppName("Statistic Summary")
    .setMaster("local[*]")

  val spark = SparkSession // init sparksession
    .builder
    .config(conf)
    .appName("readxlsx")
    .getOrCreate()

  /** Function to get the Type
    *
    * @param a
    * @tparam T
    * @return
    */
  def getType[T: TypeTag](a: T): Type = typeOf[T]

  /**
    *  Inputs for the test :  Header (list of the variable) and  statMetrics (Metrics to use)
    */
  val pathDataInitial: String = "./src/test/resources/iris.csv"

  val listContnuousAttributes: List[String] =
    Seq("SepalLength", "SepalWidth", "PetalLength", "PetalWidth").toList
  val listDiscreteAttributes: List[String] = Seq("Name").toList

  // val pathDataInitial : String = "./src/test/ressources/titanic.csv"

  // val titanicContinuousAttributes: List[String] = Seq("Fare", "Age").toList
  // val titanicDiscreteAttributes: List[String] = Seq("Survived", "Pclass","Siblings","Parents","Sex").toList

  val partialContinuousMetric: List[Metrics.ContinuousMetric] = List(Metrics.Min, Metrics.Max)

  /**
    *  Read the data .csv
    */

  val dataInitialUsed = spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load(pathDataInitial)

  /**
    * Descriptive statistics of the dataframe for Quantitative variable:
    */

  val result0 = Metrics.computeContinuousMetric(
    dataInitialUsed,
    listContnuousAttributes,
    Metrics.continuousMetrics
  )

  val result1 = Metrics.computeContinuousMetric(
    dataInitialUsed,
    listContnuousAttributes,
    partialContinuousMetric
  )

  /**
    *  1- test : Test on the mean of the dimension
    */
  val dimensionTable = (partialContinuousMetric.size + 1) * (listContnuousAttributes.size + 1)

  val dimensionDataframe = (result1.columns.size - 1) * (result1
    .select(col("Variables"))
    .collect()
    .map(_.getString(0))
    .toList
    .size + 1)

  "The size  of the Table " should "be tested" in {
    assert(dimensionTable - dimensionDataframe == 0)
  }

  /**
    *  2- test : Test for all values of the Mean
    */

  val meanList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(avg(name)).first().getDouble(0))

  val meanListTable: List[Double] = result0.select(col("Mean")).collect().map(_.getDouble(0)).toList

  "All values of The Mean " should "be tested" in {
    assert(meanList.zip(meanListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    *  3- test : Test for all values of the Min
    */

  val minList: List[Double] = listContnuousAttributes.map(
    name => dataInitialUsed.select(min(name)).first().getString(0).toDouble
  )

  val minListTable: List[Double] = result0.select(col("Min")).collect().map(_.getDouble(0)).toList

  "All values of The Min" should "be tested" in {
    assert(minList.zip(minListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    *  4- test : Test for all values of the Max
    */

  val maxList: List[Double] = listContnuousAttributes.map(
    name => dataInitialUsed.select(max(name)).first().getString(0).toDouble
  )

  val maxListTable: List[Double] = result0.select(col("Max")).collect().map(_.getDouble(0)).toList

  "All values of The Max" should "be tested" in {
    assert(maxList.zip(maxListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    *  5- test : Test for all values of the Stddev
    */

  val stddevList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(stddev(name)).first().getDouble(0))

  val stddevListTable: List[Double] =
    result0.select(col("Stddev")).collect().map(_.getDouble(0)).toList

  "All values of The Stddev" should "be tested" in {
    assert(stddevList.zip(stddevListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /**
    * 6- test : Test for all values of the Skewness
    */

  val skewnessList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(skewness(name)).first().getDouble(0))

  val skewnessListTable: List[Double] =
    result0.select(col("Skewness")).collect().map(_.getDouble(0)).toList

  "All values of The Skewness" should "be tested" in {
    assert(skewnessList.zip(skewnessListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /**
    * 7- test : Test for all values of the kurtosis
    */

  val kurtosisList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(kurtosis(name)).first().getDouble(0))

  val kurtosisListTable: List[Double] =
    result0.select(col("Kurtosis")).collect().map(_.getDouble(0)).toList

  "All values of The Kurtosis" should "be tested" in {
    assert(kurtosisList.zip(kurtosisListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

}