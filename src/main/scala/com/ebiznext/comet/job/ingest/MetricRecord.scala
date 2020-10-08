package com.ebiznext.comet.job.ingest

case class FrequencyMetricRecord(
  domain: String,
  schema: String,
  attribute: String,
  category: String,
  frequency: Long,
  count: Long,
  cometTime: Long,
  cometStage: String,
  jobId: String
) {

  override def toString: String = {
    Array(
      s"domain=$domain",
      s"schema=$schema",
      s"attribute=$attribute",
      s"category=$category",
      s"frequency=$frequency",
      s"count=$count",
      s"cometTime=$cometTime",
      s"cometStage=$cometStage",
      s"jobId=$jobId"
    ).mkString(",")
  }
}

case class DiscreteMetricRecord(
  domain: String,
  schema: String,
  attribute: String,
  missingValuesDiscrete: Long,
  countDistinct: Long,
  count: Long,
  cometTime: Long,
  cometStage: String,
  cometMetric: String,
  jobId: String
) {

  override def toString: String = {
    Array(
      s"domain=$domain",
      s"schema=$schema",
      s"countDistinct=$countDistinct",
      s"attribute=$attribute",
      s"missingValuesDiscrete=$missingValuesDiscrete",
      s"count=$count",
      s"cometTime=$cometTime",
      s"cometStage=$cometStage",
      s"cometMetric=$cometMetric",
      s"jobId=$jobId"
    ).mkString(",")
  }
}

case class ContinuousMetricRecord(
  domain: String,
  schema: String,
  attribute: String,
  min: Option[Double],
  max: Option[Double],
  mean: Option[Double],
  missingValues: Option[Long],
  standardDev: Option[Double],
  variance: Option[Double],
  sum: Option[Double],
  skewness: Option[Double],
  kurtosis: Option[Double],
  percentile25: Option[Double],
  median: Option[Double],
  percentile75: Option[Double],
  count: Long,
  cometTime: Long,
  cometStage: String,
  cometMetric: String,
  jobId: String
) {

  override def toString: String = {
    Array(
      s"domain=$domain",
      s"schema=$schema",
      s"attribute=$attribute",
      s"min=${min.map(_.toString).getOrElse("?")}",
      s"max=${max.map(_.toString).getOrElse("?")}",
      s"mean=${mean.map(_.toString).getOrElse("?")}",
      s"missingValues=${missingValues.map(_.toString).getOrElse("?")}",
      s"standardDev=${standardDev.map(_.toString).getOrElse("?")}",
      s"variance=${variance.map(_.toString).getOrElse("?")}",
      s"sum=${sum.map(_.toString).getOrElse("?")}",
      s"skewness=${skewness.map(_.toString).getOrElse("?")}",
      s"kurtosis=${kurtosis.map(_.toString).getOrElse("?")}",
      s"percentile25=${skewness.map(_.toString).getOrElse("?")}",
      s"median=${median.map(_.toString).getOrElse("?")}",
      s"percentile75=${percentile75.map(_.toString).getOrElse("?")}",
      s"count=$count",
      s"cometTime=$cometTime",
      s"cometStage=$cometStage",
      s"cometMetric=$cometMetric",
      s"jobId=$jobId"
    ).mkString(",")
  }
}
