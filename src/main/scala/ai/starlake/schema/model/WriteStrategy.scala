package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.utils.Formatter.RichFormatter
import com.fasterxml.jackson.annotation.JsonIgnore

import java.util.regex.Pattern

case class WriteStrategy(
  `type`: Option[WriteStrategyType] = None,
  types: Option[Map[String, String]] = None,
  key: List[String] = Nil,
  timestamp: Option[String] = None,
  queryFilter: Option[String] = None,
  on: Option[MergeOn] = None, // target or both (on source and target
  start_ts: Option[String] = None,
  end_ts: Option[String] = None
) {

  @JsonIgnore
  def getStrategyType(): WriteStrategyType =
    `type`.getOrElse(WriteStrategyType.APPEND)

  @JsonIgnore
  def isMerge(): Boolean =
    !Set(WriteStrategyType.APPEND, WriteStrategyType.OVERWRITE).contains(
      `type`.getOrElse(WriteStrategyType.APPEND)
    )

  def validate() = {}

  @JsonIgnore
  def getWriteMode() = `type`.getOrElse(WriteStrategyType.APPEND).toWriteMode()

  def requireKey(): Boolean = `type`.getOrElse(WriteStrategyType.APPEND).requireKey()

  def requireTimestamp(): Boolean = `type`.getOrElse(WriteStrategyType.APPEND).requireTimestamp()

  @JsonIgnore
  private val lastPat =
    Pattern.compile(".*(in)\\s+last\\(\\s*(\\d+)\\s*(\\)).*", Pattern.DOTALL)

  @JsonIgnore
  private val lastMatcher = lastPat.matcher(queryFilter.getOrElse(""))

  @JsonIgnore
  private val queryFilterContainsLast: Boolean =
    queryFilter.exists { queryFilter =>
      lastMatcher.matches()
    }
  @JsonIgnore
  private val queryFilterContainsLatest: Boolean = queryFilter.exists(_.contains("latest"))

  @JsonIgnore
  private val canOptimizeQueryFilter: Boolean = queryFilterContainsLast || queryFilterContainsLatest

  @JsonIgnore
  private val nbPartitionQueryFilter: Int =
    if (queryFilterContainsLast) lastMatcher.group(2).toInt else -1

  @JsonIgnore
  val lastStartQueryFilter: Int = if (queryFilterContainsLast) lastMatcher.start(1) else -1

  @JsonIgnore
  val lastEndQueryFilter: Int = if (queryFilterContainsLast) lastMatcher.end(3) else -1

  private def formatQuery(
    activeEnv: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty
  )(implicit
    settings: Settings
  ): Option[String] =
    queryFilter.map(_.richFormat(activeEnv, options))

  def buidlBQQuery(
    partitions: List[String],
    options: Map[String, String]
  )(implicit
    settings: Settings
  ): Option[String] = {
    val filteredPartitions = partitions.filter(!_.startsWith("__"))
    (queryFilterContainsLast, queryFilterContainsLatest) match {
      case (true, false)  => buildBQQueryForLast(filteredPartitions, options)
      case (false, true)  => buildBQQueryForLastest(filteredPartitions, options)
      case (false, false) => formatQuery(options)
      case (true, true) =>
        val last = buildBQQueryForLast(filteredPartitions, options)
        this.copy(queryFilter = last).buildBQQueryForLastest(filteredPartitions, options)
    }
  }

  private def buildBQQueryForLastest(
    partitions: List[String],
    activeEnv: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty
  )(implicit
    settings: Settings
  ): Option[String] = {
    val latestPartition = partitions.max
    val queryArgs = formatQuery(activeEnv, options).getOrElse("")
    Some(queryArgs.replace("latest", s"PARSE_DATE('%Y%m%d','$latestPartition')"))
  }

  private def buildBQQueryForLast(
    partitions: List[String],
    activeEnv: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty
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

  def compare(other: WriteStrategy): ListDiff[Named] =
    AnyRefDiff.diffAnyRef("", this, other)
}
