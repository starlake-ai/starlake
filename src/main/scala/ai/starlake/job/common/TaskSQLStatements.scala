package ai.starlake.job.common

import ai.starlake.schema.model.{ConnectionType, ExpectationSQL}
import ai.starlake.utils.JsonSerializer

import java.util
import scala.jdk.CollectionConverters._

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

  def asMap(): Map[String, Object] = {
    Map(
      "name"               -> name,
      "domain"             -> List(domain).asJava,
      "table"              -> List(table).asJava,
      "createSchemaSql"    -> createSchemaSql.asJava,
      "preActions"         -> preActions.asJava,
      "preSqls"            -> preSqls.asJava,
      "mainSqlIfExists"    -> mainSqlIfExists.asJava,
      "mainSqlIfNotExists" -> mainSqlIfNotExists.asJava,
      "postSqls"           -> postSqls.asJava,
      "addSCD2ColumnsSqls" -> addSCD2ColumnsSqls.asJava,
      "connectionType"     -> List(connectionType.toString).asJava
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
  def asMap(): Map[String, Object] = {
    val statementsAsMap = task.asMap()
    val expectationItemsAsMap = expectationItems.map(_.asMap())

    val auditAsMap = audit.map(_.asMap()).getOrElse(Map.empty)

    val aclAsMap = acl

    val expectationsAsMap = expectations.map(_.asMap()).getOrElse(Map.empty)

    Map(
      "name"             -> task.name,
      "statements"       -> statementsAsMap.asJava,
      "expectationItems" -> expectationItemsAsMap.map(_.asJava).asJava,
      "audit"            -> auditAsMap.asJava,
      "acl"              -> aclAsMap.asJava,
      "expectations"     -> expectationsAsMap.asJava
    )
  }
}
