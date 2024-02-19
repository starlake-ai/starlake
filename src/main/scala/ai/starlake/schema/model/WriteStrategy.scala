package ai.starlake.schema.model

case class WriteStrategy(
  types: Option[Map[String, String]] = None,
  `type`: Option[WriteStrategyType] = None,
  key: List[String] = Nil,
  timestamp: Option[String] = None,
  queryFilter: Option[String] = None,
  on: Option[MergeOn] = None, // target or both (on source and target
  startTs: Option[String] = None,
  endTs: Option[String] = None
)
