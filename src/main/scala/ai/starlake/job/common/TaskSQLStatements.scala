package ai.starlake.job.common

case class TaskSQLStatements(
  name: String,
  createSchemaSql: List[String],
  preActions: List[String],
  preSqls: List[String],
  mainSqlIfExists: List[String],
  mainSqlIfNotExists: List[String],
  postSqls: List[String],
  addSCD2ColumnsSqls: List[String]
) {

  def asPython(): String = {
    val map = asMap()
    val entries = map.map { case (k, list) =>
      val value = list
        .map { v =>
          s"""'''$v'''"""
        }
        .mkString(",\n")
      s""""$k": [$value]"""
    }
    s"{\n${entries.mkString(",\n")}\n}"
  }
  def asMap(): Map[String, List[String]] = {
    Map(
      "createSchemaSql"    -> createSchemaSql,
      "preActions"         -> preActions,
      "preSqls"            -> preSqls,
      "mainSqlIfExists"    -> mainSqlIfExists,
      "mainSqlIfNotExists" -> mainSqlIfNotExists,
      "postSqls"           -> postSqls,
      "addSCD2ColumnsSqls" -> addSCD2ColumnsSqls
    )
  }
}
