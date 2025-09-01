package ai.starlake.job.common

import ai.starlake.schema.model.{ConnectionType, ExpectationSQL, TableSync}

import scala.jdk.CollectionConverters.*

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
  targetSchema: List[String],
  syncStrategy: Option[TableSync],
  connectionType: ConnectionType
) {

  def asMap(): Map[String, Object] = {
    val finalSyncStrategy =
      if (targetSchema.isEmpty) TableSync.NONE else syncStrategy.getOrElse(TableSync.ADD)
    Map(
      "name"               -> name,
      "domain"             -> List(domain).asJava,
      "table"              -> List(table).asJava,
      "createSchemaSql"    -> createSchemaSql.asJava,
      "preActions"         -> preActions.asJava,
      "preSqls"            -> preSqls.asJava,
      "mainSqlIfExists"    -> mainSqlIfExists.asJava,
      "mainSqlIfNotExists" -> mainSqlIfNotExists.asJava,
      "postsql"            -> postSqls.asJava,
      "targetSchema"       -> targetSchema.asJava,
      "syncStrategy"       -> finalSyncStrategy,
      "connectionType"     -> List(connectionType.toString).asJava
    )
  }
}

case class WorkflowStatements(
  task: TaskSQLStatements,
  expectationItems: List[ExpectationSQL],
  audit: Option[TaskSQLStatements],
  acl: List[String]
) {
  def asMap(): Map[String, Object] = {
    val statementsAsMap = task.asMap()
    val expectationItemsAsMap = expectationItems.map(_.asMap())

    val auditAsMap = audit.map(_.asMap()).getOrElse(Map.empty)

    val aclAsMap = acl

    Map(
      "name"             -> task.name,
      "statements"       -> statementsAsMap.asJava,
      "expectationItems" -> expectationItemsAsMap.map(_.asJava).asJava,
      "audit"            -> auditAsMap.asJava,
      "acl"              -> aclAsMap.asJava
    )
  }
}
