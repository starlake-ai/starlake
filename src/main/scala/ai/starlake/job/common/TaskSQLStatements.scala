package ai.starlake.job.common

import ai.starlake.schema.model.{ConnectionType, ExpectationSQL}
import ai.starlake.utils.JsonSerializer

case class TaskSQLStatements(
  name: String,
  domain: String,
  table: String,
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
      "table"              -> List(table),
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

case class WorkflowStatements(
  task: TaskSQLStatements,
  expectationItems: List[ExpectationSQL],
  audit: Option[TaskSQLStatements],
  acl: List[String],
  expectations: Option[TaskSQLStatements]
) {
  def asMap(): Map[String, String] = {
    val statementsAsMap = task.asMap()
    val expectationItemsAsMap = expectationItems.map(_.asMap())

    val auditAsMap = audit.map(_.asMap()).getOrElse(Map.empty)

    val aclAsMap = acl

    val expectationsAsMap = expectations.map(_.asMap()).getOrElse(Map.empty)

    val statementsAsString =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(statementsAsMap)
    val expectationItemsAsString =
      JsonSerializer.mapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(expectationItemsAsMap)
    val auditAsString =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(auditAsMap)
    val aclAsString =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(aclAsMap)
    val expectationsAsString =
      JsonSerializer.mapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(expectationsAsMap)

    Map(
      "statements"       -> statementsAsString,
      "expectationItems" -> expectationItemsAsString,
      "audit"            -> auditAsString,
      "acl"              -> aclAsString,
      "expectations"     -> expectationsAsString
    )
  }
}
