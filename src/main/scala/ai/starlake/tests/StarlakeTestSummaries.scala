package ai.starlake.tests

case class StarlakeTestsSummary(
  name: String,
  count: Int,
  success: Int,
  failures: Int,
  successRate: Int,
  duration: Long
) {
  def getDomainName(): String = name.split('.').head
  def getTableName(): String = name.split('.').last
  // getters for jinjava
  def getName(): String = name
  def getCount(): Int = count
  def getSuccess(): Int = success
  def getFailures(): Int = failures
  def getSuccessRate(): Int = successRate
  def getDuration(): String = {
    val d: Double = (duration.toDouble / 100).round / 10.0 // rounded to 1 decimal
    s"$d"
  }

  def asMap(): Map[String, Any] = {
    Map(
      "name"        -> name,
      "count"       -> count,
      "success"     -> success,
      "failures"    -> failures,
      "successRate" -> successRate,
      "duration"    -> duration
    )
  }
}

object StarlakeTestsSummary {
  def summaries(results: List[StarlakeTestResult]): StarlakeTestsSummary = {
    val count = results.size
    val failures = results.count(!_.success)
    val success = results.count(_.success)
    val duration = results.map(_.duration).sum
    val successRate = if (count == 0) 0 else success.toDouble * 100 / count
    val domainNames = results.map(_.domainName).distinct.mkString(",")
    StarlakeTestsSummary(domainNames, count, success, failures, successRate.toInt, duration)
  }

  def summaryIndex(results: List[StarlakeTestsSummary]): StarlakeTestsSummary = {
    val empty = StarlakeTestsSummary("all", 0, 0, 0, 0, 0)
    val result = results.fold(empty) { (acc: StarlakeTestsSummary, r: StarlakeTestsSummary) =>
      val count = acc.count + r.count
      val success = acc.success + r.success
      val failures = acc.failures + r.failures
      val duration = acc.duration + r.duration
      StarlakeTestsSummary("all", count, success, failures, 0, duration)
    }
    val successRate = if (result.count == 0) 0 else result.success.toDouble * 100 / result.count
    result.copy(successRate = successRate.toInt)
  }
}

object StarlakeTestsDomainSummary {
  def summaries(results: List[StarlakeTestResult]): List[StarlakeTestsSummary] = {
    results
      .groupBy(_.domainName)
      .map { case (domain, tests) =>
        val count = tests.size
        val failures = tests.count(!_.success)
        val success = tests.count(_.success)
        val duration = tests.map(_.duration).sum
        val successRate = if (count == 0) 0 else success.toDouble * 100 / count
        StarlakeTestsSummary(domain, count, success, failures, successRate.toInt, duration)
      }
      .toList
  }
}
object StarlakeTestsTableSummary {
  def summaries(
    domainName: String,
    results: List[StarlakeTestResult]
  ): List[StarlakeTestsSummary] = {
    results
      .filter(_.domainName == domainName)
      .groupBy(_.taskName)
      .map { case (taskName, tests) =>
        val count = tests.size
        val failures = tests.count(!_.success)
        val success = tests.count(_.success)
        val duration = tests.map(_.duration).sum
        val successRate = if (count == 0) 0 else success.toDouble * 100 / count
        StarlakeTestsSummary(
          s"$domainName.$taskName",
          count,
          success,
          failures,
          successRate.toInt,
          duration
        )
      }
      .toList
  }
}
