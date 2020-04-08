package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.job.metrics.Metrics._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class MetricsJobSpec extends TestHelper {

  /**
    * Inputs for the test :  Header (list of the variable) and Metrics (Metrics to use)
    */

  val listContnuousAttributes: List[String] =
    Seq("SepalLength", "SepalWidth", "PetalLength", "PetalWidth").toList
  val listDiscreteAttributes: List[String] = Seq("Name").toList

  val partialContinuousMetric: List[ContinuousMetric] = List(Min, Max)

  /**
    * schema of metrics
    */
  val expectedMetricsSchema = StructType(
    Array(
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("attribute", StringType, nullable = true),
      StructField("min", DoubleType, nullable = true),
      StructField("max", DoubleType, nullable = true),
      StructField("mean", DoubleType, nullable = true),
      StructField("missingValues", LongType, nullable = true),
      StructField("standardDev", DoubleType, nullable = true),
      StructField("variance", DoubleType, nullable = true),
      StructField("sum", DoubleType, nullable = true),
      StructField("skewness", DoubleType, nullable = true),
      StructField("kurtosis", DoubleType, nullable = true),
      StructField("percentile25", DoubleType, nullable = true),
      StructField("median", DoubleType, nullable = true),
      StructField("percentile75", DoubleType, nullable = true),
      StructField("countDistinct", LongType, nullable = true),
      StructField(
        "catCountFreq",
        ArrayType(
          StructType(
            Array(
              StructField("category", StringType, nullable = true),
              StructField("count", LongType, nullable = true),
              StructField("frequency", DoubleType, nullable = true)
            )
          )
        ),
        nullable = true
      ),
      StructField("missingValuesDiscrete", LongType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("cometTime", LongType, nullable = true),
      StructField("cometStage", StringType, nullable = true)
    )
  )

  /**
    * Read the data .csv
    */
  lazy val dataInitialUsed = {

    val value = sparkSession.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load("./src/test/resources/iris.csv")

    /**
      * Descriptive statistics of the dataframe for Quantitative variable:
      */
    value.printSchema()
    value
  }

  lazy val result0 = computeContinuousMetric(
    dataInitialUsed,
    listContnuousAttributes,
    continuousMetrics
  )

  lazy val result1 = computeContinuousMetric(
    dataInitialUsed,
    listContnuousAttributes,
    partialContinuousMetric
  )

  /**
    * 1- test : Test on the mean of the dimension
    */
  lazy val dimensionTable = {
    val dimensionTable =
      (partialContinuousMetric.size + 1) * (listContnuousAttributes.size + 1)
    logger.info(s"-->$dimensionTable")

    dimensionTable
  }

  lazy val dimensionDataframe = result1.map { result1 =>
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
  lazy val meanList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(avg(name)).first().getDouble(0))

  lazy val meanListTable: List[Double] = result0.map { result0 =>
      result0.select(col("mean")).collect().map(_.getDouble(0)).toList
    } getOrElse (Nil)

  "All values of The Mean " should "be tested" in {
    assert(meanList.zip(meanListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    * 3- test : Test for all values of the Min
    */
  lazy val minList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(min(name)).first().getDouble(0))

  lazy val minListTable: List[Double] = result0.map { result0 =>
      result0.select(col("min")).collect().map(_.getDouble(0)).toList
    } getOrElse (Nil)

  "All values of The Min" should "be tested" in {
    assert(minList.zip(minListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    * 4- test : Test for all values of the Max
    */
  lazy val maxList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(max(name)).first().getDouble(0))

  lazy val maxListTable: List[Double] = result0.map { result0 =>
      result0.select(col("max")).collect().map(_.getDouble(0)).toList
    } getOrElse (Nil)

  "All values of The Max" should "be tested" in {
    assert(maxList.zip(maxListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /**
    * 5- test : Test for all values of the standardDev
    */
  lazy val stddevList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(stddev(name)).first().getDouble(0))

  lazy val stddevListTable: List[Double] = result0.map { result0 =>
      result0.select(col("standardDev")).collect().map(_.getDouble(0)).toList
    } getOrElse Nil

  "All values of The standardDev" should "be tested" in {
    assert(stddevList.zip(stddevListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /**
    * 6- test : Test for all values of the Skewness
    */
  lazy val skewnessList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(skewness(name)).first().getDouble(0))

  lazy val skewnessListTable: List[Double] = result0.map { result0 =>
      result0.select(col("skewness")).collect().map(_.getDouble(0)).toList
    } getOrElse (Nil)

  "All values of The Skewness" should "be tested" in {
    assert(skewnessList.zip(skewnessListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /**
    * 7- test : Test for all values of the kurtosis
    */
  lazy val kurtosisList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(kurtosis(name)).first().getDouble(0))

  lazy val kurtosisListTable: List[Double] = result0.map { result0 =>
      result0.select(col("kurtosis")).collect().map(_.getDouble(0)).toList
    } getOrElse (Nil)

  "All values of The Kurtosis" should "be tested" in {
    assert(kurtosisList.zip(kurtosisListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  new WithSettings() {
    "Yelp Business Metrics" should "produce correct metrics" in {
      new SpecTrait(
        domainFilename = "yelp.yml",
        sourceDomainPathname = s"/sample/yelp/yelp.yml",
        datasetDomainName = "yelp",
        sourceDatasetPathName = "/sample/yelp/business.json"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending
        val countAccepted: Long = sparkSession.read
          .parquet(cometDatasetsPath + s"/accepted/$datasetDomainName/business")
          .count()

        val path: Path = DatasetArea.metrics("yelp", "business")
        val metricsDf: DataFrame = sparkSession.read.parquet(path.toString)
        metricsDf.schema shouldBe expectedMetricsSchema
        import sparkSession.implicits._
        val metricsSelectedColumns =
          metricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .take(7)
        metricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "city"),
          ("yelp", "business", "is_open"),
          ("yelp", "business", "postal_code"),
          ("yelp", "business", "state"),
          ("yelp", "business", "review_count"),
          ("yelp", "business", "stars"),
          ("yelp", "business", "is_open")
        )
      }
    }
  }
}
