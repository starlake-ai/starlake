package ai.starlake.job.common

import ai.starlake.schema.model.ConnectionType

case class TaskSQLStatements(
  name: String,
  domain: String,
  createSchemaSql: List[String],
  preActions: List[String],
  preSqls: List[String],
  mainSqlIfExists: List[String],
  mainSqlIfNotExists: List[String],
  postSqls: List[String],
  addSCD2ColumnsSqls: List[String],
  connectionType: ConnectionType
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
      "domain"             -> List(domain),
      "createSchemaSql"    -> createSchemaSql,
      "preActions"         -> preActions,
      "preSqls"            -> preSqls,
      "mainSqlIfExists"    -> mainSqlIfExists,
      "mainSqlIfNotExists" -> mainSqlIfNotExists,
      "postSqls"           -> postSqls,
      "addSCD2ColumnsSqls" -> addSCD2ColumnsSqls,
      "connectionType"     -> List(connectionType.toString)
    )
  }
}
