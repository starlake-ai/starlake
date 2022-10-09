package ai.starlake.schema.model

import ai.starlake.config.Settings
import com.fasterxml.jackson.annotation.JsonIgnore

import java.util.regex.Pattern
import ai.starlake.utils.Formatter._

/** How dataset are merged
  *
  * @param key
  *   list of attributes to join existing with incoming dataset. Use renamed columns here.
  * @param delete
  *   Optional valid sql condition on the incoming dataset. Use renamed column here.
  * @param timestamp
  *   Timestamp column used to identify last version, if not specified currently ingested row is
  *   considered the last. Maybe prefixed with TIMESTAMP or DATE(default) to specifiy if it is a
  *   timestamp or a date (useful on dynamic partitioning on BQ to selectively apply PARSE_DATE or
  *   PARSE_TIMESTAMP
  */
case class MergeOptions(
  key: List[String],
  delete: Option[String] = None,
  timestamp: Option[String] = None,
  queryFilter: Option[String] = None
) {
  @JsonIgnore
  private val lastPat =
    Pattern.compile(".*(in)\\s+last\\(\\s*(\\d+)\\s*(\\)).*", Pattern.DOTALL)

  @JsonIgnore
  private val matcher = lastPat.matcher(queryFilter.getOrElse(""))

  @JsonIgnore
  private val queryFilterContainsLast: Boolean =
    queryFilter.exists { queryFilter =>
      matcher.matches()
    }
  @JsonIgnore
  private val queryFilterContainsLatest: Boolean = queryFilter.exists(_.contains("latest"))

  @JsonIgnore
  private val canOptimizeQueryFilter: Boolean = queryFilterContainsLast || queryFilterContainsLatest

  @JsonIgnore
  private val nbPartitionQueryFilter: Int =
    if (queryFilterContainsLast) matcher.group(2).toInt else -1

  @JsonIgnore
  val lastStartQueryFilter: Int = if (queryFilterContainsLast) matcher.start(1) else -1

  @JsonIgnore
  val lastEndQueryFilter: Int = if (queryFilterContainsLast) matcher.end(3) else -1

  private def formatQuery(activeEnv: Map[String, String], options: Map[String, String])(implicit
    settings: Settings
  ): Option[String] =
    queryFilter.map(_.richFormat(activeEnv, options))

  def buidlBQQuery(
    partitions: List[String],
    activeEnv: Map[String, String],
    options: Map[String, String]
  )(implicit
    settings: Settings
  ): Option[String] = {
    (queryFilterContainsLast, queryFilterContainsLatest) match {
      case (true, false)  => buildBQQueryForLast(partitions, activeEnv, options)
      case (false, true)  => buildBQQueryForLastest(partitions, activeEnv, options)
      case (false, false) => formatQuery(activeEnv, options)
      case (true, true) =>
        val last = buildBQQueryForLast(partitions, activeEnv, options)
        this.copy(queryFilter = last).buildBQQueryForLastest(partitions, activeEnv, options)
    }
  }

  private def buildBQQueryForLastest(
    partitions: List[String],
    activeEnv: Map[String, String],
    options: Map[String, String]
  )(implicit
    settings: Settings
  ): Option[String] = {
    val latestPartition = partitions.max
    val queryArgs = formatQuery(activeEnv, options).getOrElse("")
    Some(queryArgs.replace("latest", s"PARSE_DATE('%Y%m%d','$latestPartition')"))
  }

  private def buildBQQueryForLast(
    partitions: List[String],
    activeEnv: Map[String, String],
    options: Map[String, String]
  )(implicit
    settings: Settings
  ): Option[String] = {
    val sortedPartitions = partitions.sorted
    val (oldestPartition, newestPartition) = if (sortedPartitions.length < nbPartitionQueryFilter) {
      (
        sortedPartitions.headOption.getOrElse("19700101"),
        sortedPartitions.lastOption.getOrElse("19700101")
      )
    } else {
      (
        sortedPartitions(sortedPartitions.length - nbPartitionQueryFilter),
        sortedPartitions.last
      )

    }
    val lastStart = lastStartQueryFilter
    val lastEnd = lastEndQueryFilter
    val queryArgs = formatQuery(activeEnv, options)
    queryArgs.map { queryArgs =>
      queryArgs
        .substring(
          0,
          lastStart
        ) + s"between PARSE_DATE('%Y%m%d','$oldestPartition') and PARSE_DATE('%Y%m%d','$newestPartition')" + queryArgs
        .substring(lastEnd)
    }
  }
}
