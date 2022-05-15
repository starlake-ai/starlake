package ai.starlake.job.metrics

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.ingest.{ContinuousMetricRecord, DiscreteMetricRecord, FrequencyMetricRecord}
import ai.starlake.job.metrics.Metrics._
import ai.starlake.{JdbcChecks, TestHelper}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class MetricsJobSpec extends TestHelper with JdbcChecks {

  /** Inputs for the test : Header (list of the variable) and Metrics (Metrics to use)
    */

  val listContnuousAttributes: List[String] =
    Seq("SepalLength", "SepalWidth", "PetalLength", "PetalWidth").toList
  val listDiscreteAttributes: List[String] = Seq("Name").toList

  val partialContinuousMetric: List[ContinuousMetric] = List(Min, Max)

  /** schema of metrics
    */
  val expectedDiscreteMetricsSchema = StructType(
    Array(
      StructField("attribute", StringType, nullable = true),
      StructField("countDistinct", LongType, nullable = true),
      StructField("missingValuesDiscrete", LongType, nullable = true),
      StructField("cometMetric", StringType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("cometTime", LongType, nullable = true),
      StructField("cometStage", StringType, nullable = true)
    )
  )

  val expectedContinuousMetricsSchema = StructType(
    Array(
      StructField("attribute", StringType, nullable = true),
      StructField("min", DoubleType, nullable = true),
      StructField("max", DoubleType, nullable = true),
      StructField("mean", DoubleType, nullable = true),
      StructField("missingValues", LongType, nullable = true),
      StructField("variance", DoubleType, nullable = true),
      StructField("standardDev", DoubleType, nullable = true),
      StructField("sum", DoubleType, nullable = true),
      StructField("skewness", DoubleType, nullable = true),
      StructField("kurtosis", DoubleType, nullable = true),
      StructField("percentile25", DoubleType, nullable = true),
      StructField("median", DoubleType, nullable = true),
      StructField("percentile75", DoubleType, nullable = true),
      StructField("cometMetric", StringType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("cometTime", LongType, nullable = true),
      StructField("cometStage", StringType, nullable = true)
    )
  )

  val expectedFreqMetricsSchema = StructType(
    Array(
      StructField("attribute", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("frequency", DoubleType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("cometTime", LongType, nullable = true),
      StructField("cometStage", StringType, nullable = true)
    )
  )

  /** Read the data .csv
    */
  lazy val dataInitialUsed = {

    val value = sparkSession.read
      .format("csv")
      .option("header", "true") // reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load("./src/test/resources/iris.csv")

    /** Descriptive statistics of the dataframe for Quantitative variable:
      */
    logger.info(value.schemaString())
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

  /** 1- test : Test on the mean of the dimension
    */
  lazy val dimensionTable = {
    val dimensionTable =
      (partialContinuousMetric.size + 1) * (listContnuousAttributes.size + 1)
    logger.info(s"-->$dimensionTable")

    dimensionTable
  }

  lazy val dimensionDataframe = result1.map { result1 =>
    logger.info(result1.schemaString())
    (result1.columns.length - 1) * (result1
      .select(col("attribute"))
      .collect()
      .map(_.getString(0))
      .toList
      .size + 1)
  }

  "The size  of the Table " should "be tested" in {
    assert(dimensionTable - dimensionDataframe.getOrElse(0) == 0)
  }

  /** 2- test : Test for all values of the Mean
    */
  lazy val meanList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(avg(name)).first().getDouble(0))

  lazy val meanListTable: List[Double] = result0.map { result0 =>
    result0.select(col("mean")).collect().map(_.getDouble(0)).toList
  } getOrElse Nil

  "All values of The Mean " should "be tested" in {
    assert(meanList.zip(meanListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /** 3- test : Test for all values of the Min
    */
  lazy val minList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(min(name)).first().getDouble(0))

  lazy val minListTable: List[Double] = result0.map { result0 =>
    result0.select(col("min")).collect().map(_.getDouble(0)).toList
  } getOrElse Nil

  "All values of The Min" should "be tested" in {
    assert(minList.zip(minListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /** 4- test : Test for all values of the Max
    */
  lazy val maxList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(max(name)).first().getDouble(0))

  lazy val maxListTable: List[Double] = result0.map { result0 =>
    result0.select(col("max")).collect().map(_.getDouble(0)).toList
  } getOrElse Nil

  "All values of The Max" should "be tested" in {
    assert(maxList.zip(maxListTable).map(x => x._1 - x._2).sum <= 0.00001)
  }

  /** 5- test : Test for all values of the standardDev
    */
  lazy val stddevList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(stddev(name)).first().getDouble(0))

  lazy val stddevListTable: List[Double] = result0.map { result0 =>
    result0.select(col("standardDev")).collect().map(_.getDouble(0)).toList
  } getOrElse Nil

  "All values of The standardDev" should "be tested" in {
    assert(stddevList.zip(stddevListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /** 6- test : Test for all values of the Skewness
    */
  lazy val skewnessList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(skewness(name)).first().getDouble(0))

  lazy val skewnessListTable: List[Double] = result0.map { result0 =>
    result0.select(col("skewness")).collect().map(_.getDouble(0)).toList
  } getOrElse Nil

  "All values of The Skewness" should "be tested" in {
    assert(skewnessList.zip(skewnessListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  /** 7- test : Test for all values of the kurtosis
    */
  lazy val kurtosisList: List[Double] =
    listContnuousAttributes.map(name => dataInitialUsed.select(kurtosis(name)).first().getDouble(0))

  lazy val kurtosisListTable: List[Double] = result0.map { result0 =>
    result0.select(col("kurtosis")).collect().map(_.getDouble(0)).toList
  } getOrElse Nil

  "All values of The Kurtosis" should "be tested" in {
    assert(kurtosisList.zip(kurtosisListTable).map(x => x._1 - x._2).sum <= 0.001)
  }

  new WithSettings {
    "Yelp Business Metrics" should "produce correct metrics in parquet file" in {
      new SpecTrait(
        domainOrJobFilename = "yelp.comet.yml",
        sourceDomainOrJobPathname = s"/sample/yelp/yelp.comet.yml",
        datasetDomainName = "yelp",
        sourceDatasetPathName = "/sample/yelp/business.json"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending

        val discretePath: Path = DatasetArea.discreteMetrics("yelp", "business")
        val discreteMetricsDf: DataFrame = sparkSession.read.parquet(discretePath.toString)
        logger.info(discreteMetricsDf.showString(truncate = 0))
        discreteMetricsDf.schema shouldBe expectedDiscreteMetricsSchema
        import sparkSession.implicits._
        val discreteMetricsSelectedColumns =
          discreteMetricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .take(7)
        discreteMetricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "city"),
          ("yelp", "business", "is_open"),
          ("yelp", "business", "postal_code"),
          ("yelp", "business", "state"),
          ("yelp", "business", "is_open")
        )

        val continuousPath: Path = DatasetArea.continuousMetrics("yelp", "business")
        val continuousMetricsDf: DataFrame = sparkSession.read.parquet(continuousPath.toString)
        logger.info(continuousMetricsDf.showString(truncate = 0))
        continuousMetricsDf.schema shouldBe expectedContinuousMetricsSchema
        import sparkSession.implicits._
        val continuousMetricsSelectedColumns =
          continuousMetricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .take(7)

        continuousMetricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "review_count"),
          ("yelp", "business", "stars")
        )

        val freqPath: Path = DatasetArea.frequenciesMetrics("yelp", "business")
        val freqMetricsDf: DataFrame = sparkSession.read.parquet(freqPath.toString)
        logger.info(freqMetricsDf.showString(truncate = 0))
        freqMetricsDf.schema shouldBe expectedFreqMetricsSchema
        import sparkSession.implicits._
        val freqMetricsSelectedColumns =
          freqMetricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .take(7)

        freqMetricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "city")
        )
      }
    }
    "All Metrics Config" should "be known and taken  into account" in {
      val rendered = MetricsConfig.usage()
      val expected =
        """
          |Usage: starlake metrics [options]
          |
          |  --domain <value>  Domain Name
          |  --schema <value>  Schema Name
          |  --stage <value>   Stage (UNIT or GLOBAL)
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }

  val jdbcConfiguration: Config =
    ConfigFactory
      .parseString("""
                     |metrics {
                     |  active = true
                     |  sink {
                     |    type = "JdbcSink"
                     |    connection = "test-h2"
                     |  }
                     |}
                     |
                     |audit {
                     |  active = true
                     |  sink {
                     |    type = "JdbcSink"
                     |    connection = "test-h2"
                     |  }
                     |}
                     |""".stripMargin)
      .withFallback(super.testConfiguration)

  private def expectedMetricRecords(implicit
    settings: Settings
  ): (List[ContinuousMetricRecord], List[DiscreteMetricRecord], List[FrequencyMetricRecord]) =
    (
      List(
        ContinuousMetricRecord(
          domain = "yelp",
          schema = "business",
          attribute = "review_count",
          min = Some(3.0),
          max = Some(664.0),
          mean = Some(38.675),
          missingValues = Some(0),
          standardDev = Some(89.303),
          variance = Some(7974.944),
          sum = Some(7735.0),
          skewness = Some(4.359),
          kurtosis = Some(21.423),
          percentile25 = Some(4.359),
          median = Some(9.0),
          percentile75 = Some(25.0),
          count = 200,
          cometTime = 1602103587981L,
          cometStage = "UNIT",
          cometMetric = "Continuous",
          jobId = "296e668b-5748-4ad1-801e-6ce2aa3bd5d6"
        ),
        ContinuousMetricRecord(
          domain = "yelp",
          schema = "business",
          attribute = "stars",
          min = Some(1.0),
          max = Some(5.0),
          mean = Some(3.692),
          missingValues = Some(0),
          standardDev = Some(1.006),
          variance = Some(1.012),
          sum = Some(738.5),
          skewness = Some(-0.613),
          kurtosis = Some(-0.145),
          percentile25 = Some(-0.613),
          median = Some(4.0),
          percentile75 = Some(4.5),
          count = 200,
          cometTime = 1602103587981L,
          cometStage = "UNIT",
          cometMetric = "Continuous",
          jobId = "296e668b-5748-4ad1-801e-6ce2aa3bd5d6"
        )
      ).map(x => x.copy(cometTime = 0L, jobId = "")),
      List(
        DiscreteMetricRecord(
          domain = "yelp",
          schema = "business",
          countDistinct = 53,
          attribute = "city",
          missingValuesDiscrete = 0,
          count = 200,
          cometTime = 1602157742857L,
          cometStage = "UNIT",
          cometMetric = "Discrete",
          jobId = "2f811367-0d9f-4481-b9a2-fd4d87fe795f"
        ),
        DiscreteMetricRecord(
          domain = "yelp",
          schema = "business",
          countDistinct = 2,
          attribute = "is_open",
          missingValuesDiscrete = 0,
          count = 200,
          cometTime = 1602157742857L,
          cometStage = "UNIT",
          cometMetric = "Discrete",
          jobId = "2f811367-0d9f-4481-b9a2-fd4d87fe795f"
        ),
        DiscreteMetricRecord(
          domain = "yelp",
          schema = "business",
          countDistinct = 158,
          attribute = "postal_code",
          missingValuesDiscrete = 0,
          count = 200,
          cometTime = 1602157742857L,
          cometStage = "UNIT",
          cometMetric = "Discrete",
          jobId = "2f811367-0d9f-4481-b9a2-fd4d87fe795f"
        ),
        DiscreteMetricRecord(
          domain = "yelp",
          schema = "business",
          countDistinct = 9,
          attribute = "state",
          missingValuesDiscrete = 0,
          count = 200,
          cometTime = 1602157742857L,
          cometStage = "UNIT",
          cometMetric = "Discrete",
          jobId = "2f811367-0d9f-4481-b9a2-fd4d87fe795f"
        )
      ),
      List(
        FrequencyMetricRecord(
          domain = "yelp",
          schema = "business",
          attribute = "city",
          category = "Tempe",
          frequency = 0,
          count = 200,
          cometTime = 1602157958121L,
          cometStage = "UNIT",
          jobId = "43bd4c3f-43c9-417b-bc1d-4aaf72415736"
        ),
        FrequencyMetricRecord(
          domain = "yelp",
          schema = "business",
          attribute = "city",
          category = "North Las Vegas",
          frequency = 0,
          count = 200,
          cometTime = 1602157958121L,
          cometStage = "UNIT",
          jobId = "43bd4c3f-43c9-417b-bc1d-4aaf72415736"
        )
      )
    )

  new WithSettings(jdbcConfiguration) {
    "Yelp Business Metrics" should "produce correct metrics in JDBC database" in {
      new SpecTrait(
        domainOrJobFilename = "yelp.comet.yml",
        sourceDomainOrJobPathname = s"/sample/yelp/yelp.comet.yml",
        datasetDomainName = "yelp",
        sourceDatasetPathName = "/sample/yelp/business.json"
      ) {
        cleanMetadata
        cleanDatasets
        loadPending

        val discretePath: Path = DatasetArea.discreteMetrics("yelp", "business")
        val discreteMetricsDf: DataFrame = sparkSession.read.parquet(discretePath.toString)
        logger.info(discreteMetricsDf.showString(truncate = 0))
        discreteMetricsDf.schema shouldBe expectedDiscreteMetricsSchema

        import sparkSession.implicits._

        val discreteMetricsSelectedColumns =
          discreteMetricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .take(7)
        discreteMetricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "city"),
          ("yelp", "business", "is_open"),
          ("yelp", "business", "postal_code"),
          ("yelp", "business", "state"),
          ("yelp", "business", "is_open")
        )

        val (continuous, discrete, frequencies) = expectedMetricRecords(settings)
        expectingMetrics("test-h2", continuous, discrete, frequencies)
      }
    }
  }
}
