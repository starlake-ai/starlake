package ai.starlake.job.metrics

import ai.starlake.config.DatasetArea
import ai.starlake.{JdbcChecks, TestHelper}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

class MetricsJobSpec extends TestHelper with JdbcChecks {
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
      StructField("timestamp", LongType, nullable = true)
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
      StructField("timestamp", LongType, nullable = true)
    )
  )
  val expectedDiscreteMetricsSchema = StructType(
    Array(
      StructField(
        "attribute",
        StringType,
        nullable = true
      ),
      StructField("countDistinct", LongType, nullable = true),
      StructField(
        "missingValuesDiscrete",
        LongType,
        nullable = true
      ),
      StructField("cometMetric", StringType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    )
  )

  val expectedDiscreteMetricsJDBCSchema: StructType = StructType(
    expectedDiscreteMetricsSchema.fields.map(field =>
      field.copy(metadata = Metadata.fromJson("""{"scale":0}"""))
    )
  )

  val sparkConfiguration: Config = {
    val config = ConfigFactory
      .parseString("""
          |connectionRef = "spark"
          |audit.sink.connectionRef = "spark"
          |""".stripMargin)
    val result = config.withFallback(super.testConfiguration)
    result
  }

  new WithSettings(sparkConfiguration) {
    "Yelp Business Metrics" should "produce correct metrics in parquet file" in {
      new SpecTrait(
        domainOrJobFilename = "yelp.sl.yml",
        sourceDomainOrJobPathname = s"/sample/yelp/yelp.sl.yml",
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

        val session = sparkSession

        import session.implicits._

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
        val continuousMetricsSelectedColumns: Array[(String, String, String)] =
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

        val freqMetricsSelectedColumns: Array[(String, String, String)] =
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
          |  --domain <value>   Domain Name
          |  --schema <value>   Schema Name
          |  --authInfo <value> Auth Info.  Google Cloud use: gcpProjectId and gcpSAJsonKey
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }

}
