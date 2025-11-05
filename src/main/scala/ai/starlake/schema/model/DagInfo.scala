package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.job.metrics.ExpectationJob
import ai.starlake.job.transform.AutoTaskQueries
import ai.starlake.lineage.TaskViewDependencyNode
import ai.starlake.schema.generator.Yml2DagTemplateLoader
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JsonSerializer, Utils}

import java.util
import java.util.Calendar
import scala.jdk.CollectionConverters.*
import scala.util.Try

case class DagDesc(version: Int, dag: DagInfo)

case class DagSchedule(
  schedule: String,
  cron: String,
  domains: java.util.List[DagDomain],
  rawDomains: java.util.List[java.util.Map[String, Any]]
) {
  def getSchedule(): String = schedule

  def getCron(): String = cron

  def getDomains(): java.util.List[DagDomain] = domains

  def getRawDomains(): util.List[java.util.Map[String, Any]] = rawDomains
}

case class DagDomain(name: String, finalName: String, tables: java.util.List[TableDomain]) {
  def getName(): String = name

  def getFinalName(): String = finalName

  def getTables(): java.util.List[TableDomain] = tables
}

case class TableDomain(name: String, finalName: String) {
  def getName(): String = name

  def getFinalName(): String = finalName
}

case class DagPair(name: String, value: String) {
  def getName(): String = name
  def getValue(): String = value
}

case class DagInfo(
  comment: String,
  template: String,
  filename: String,
  options: Map[String, String]
) {
  def getSlRoot()(implicit settings: Settings): String = {
    val slRoot =
      this.options
        .get("sl_env_var")
        .flatMap { envVar =>
          // parse envVar as Json
          JsonSerializer.mapper
            .readValue(envVar, classOf[Map[String, String]])
            .get("SL_ROOT")
        }
        .getOrElse(settings.appConfig.root)
    // In case SL_ROOT is defined using the SL_ROOT env var
    Utils.parseJinja(slRoot, Map("SL_ROOT" -> settings.appConfig.root))

  }

  def checkValidity()(implicit settings: Settings): List[ValidationMessage] = {
    var errors = List.empty[ValidationMessage]
    if (template.isEmpty) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "template",
        "Template is required"
      )
    }
    Try(new Yml2DagTemplateLoader().loadTemplate(template)).getOrElse {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "template",
        s"DAG Template '$template' not found"
      )
    }
    if (filename.isEmpty) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "filename",
        "Filename is required"
      )
    }

    if (template.contains("..")) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "template",
        "Template cannot contain '..'"
      )
    }
    if (template.startsWith("/") && !template.startsWith(settings.appConfig.root)) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "template",
        "Template cannot start with '/'"
      )
    }
    if (filename.contains("..")) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "filename",
        "Filename cannot contain '..'"
      )
    }
    if (filename.startsWith("/") && !filename.startsWith(settings.appConfig.root)) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "filename",
        "Filename cannot start with '/'"
      )
    }
    options.foreach { case (k, v) =>
      if (k.isEmpty) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "options",
          "Option key is required"
        )
      }
      if (k.contains("..")) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "options",
          "Option key cannot contain '..'"
        )
      }
      if (k.startsWith("/") && !k.startsWith(settings.appConfig.root)) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "options",
          "Option key cannot start with '/'"
        )
      }
      if (v.isEmpty) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "options",
          "Option value is required"
        )
      }
      if (v.contains("..")) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "options",
          "Option value cannot contain '..'"
        )
      }
      if (v.startsWith("/") && !v.startsWith(settings.appConfig.root)) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "options",
          "Option value cannot start with '/'"
        )
      }
    }
    errors
  }

  def getfilenameVars()(implicit settings: Settings): Set[String] = filename.extractVars()

  def this() = this("", "", "", Map.empty)

  def this(template: String, filename: String, options: Map[String, String]) =
    this("", template, filename, options)

  def asMap: util.HashMap[String, Object] = {
    val jOptions = new java.util.ArrayList[DagPair]
    options.foreach { case (k, v) =>
      jOptions.add(DagPair(k, v))
    }
    new java.util.HashMap[String, Object]() {
      put("comment", comment)
      put("template", template)
      put("options", jOptions)
    }
  }
}

object DagInfo {
  val externalKeys =
    Set(
      "name",
      "statements",
      "expectationItems",
      "audit",
      "acl",
      "expectations",
      "schedules",
      "connection",
      "config"
    )
}
case class LoadDagGenerationContext(
  config: DagInfo,
  schedules: List[DagSchedule],
  workflowStatementsIn: List[Map[String, Object]]
) {
  val workflowStatements = workflowStatementsIn.filter(_.size > 0)
  def asMap()(implicit settings: Settings): util.HashMap[String, Object] = {
    val updatedOptions = if (!config.options.contains("SL_TIMEZONE")) {
      val cal1 = Calendar.getInstance
      val tz = cal1.getTimeZone();
      config.options + ("SL_TIMEZONE" -> tz.getID)
    } else {
      config.options
    }
    val updatedConfig = config.copy(options = updatedOptions)
    val statementsAsMap = workflowStatements.map { it =>
      val map = it("statements").asInstanceOf[java.util.Map[String, Object]]
      it("name") -> map
    }.toMap
    val statementsAsString =
      JsonSerializer.mapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(statementsAsMap)

    val expectationItemsAsMap = workflowStatements.map { it =>
      val map = it("expectationItems").asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      it("name") -> map
    }.toMap

    val expectationItemsAsString =
      JsonSerializer.mapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(expectationItemsAsMap)

    val auditAsMap = workflowStatements
      .map { it =>
        it("audit")
      }
      .headOption
      .getOrElse(Map.empty)

    val auditAsString =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(auditAsMap)

    val aclAsMap = workflowStatements.map { it =>
      val map = it("acl").asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      it("name") -> map
    }
    val aclAsString =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(aclAsMap)

    val connectionsAsMap = workflowStatements.map { it =>
      val map = it("connection")
      it("name") -> map
    }.toMap
    val connectionsAsString =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(connectionsAsMap)

    val expectations = ExpectationJob.buildSQLStatements()
    val expectationsAsMap = expectations.map(_.asMap()).getOrElse(Map.empty)

    val expectationsAsString =
      JsonSerializer.mapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(expectationsAsMap)

    val res = new java.util.HashMap[String, Object]() {

      workflowStatements.foreach { tableConfig =>
        val tableAsJava = new java.util.HashMap[String, Object]() {
          tableConfig.foreach { case (k, v) =>
            if (!DagInfo.externalKeys.contains(k))
              put(k, v)
          }
        }
        if (!tableAsJava.isEmpty)
          put(tableConfig("name").toString, tableAsJava)
      }
      put("config", updatedConfig.asMap)
      put("schedules", schedules.asJava)

      put("statements", statementsAsString)
      put("expectationItems", expectationItemsAsString)
      put("audit", auditAsString)
      put("expectations", expectationsAsString)
      put("acl", aclAsString)
      put("connection", connectionsAsString)

    }
    res
  }
}
case class TransformDagGenerationContext(
  config: DagInfo,
  deps: List[TaskViewDependencyNode],
  cron: Option[String],
  statements: List[AutoTaskQueries]
) {
  def asMap()(implicit settings: Settings): util.HashMap[String, Object] = {
    val statementsAsMap = statements.map { atq =>
      atq.statements.name -> atq.statements.asMap()
    }.toMap

    val expectationItemsAsMap = statements.map { atq =>
      atq.statements.name -> atq.expectationItems.map(_.asMap()).asJava
    }.toMap

    val connectionsAsMap = statements.map { atq =>
      atq.statements.name -> atq.connectionOptions.asJava
    }.toMap

    val auditAsMap = statements
      .flatMap { atq => atq.audit.map(_.asMap()) }
      .headOption
      .getOrElse(Map.empty)

    val aclAsMap = statements.flatMap { atq =>
      atq.audit.map { audit => atq.statements.name -> atq.acl }
    }.toMap

    val expectations = ExpectationJob.buildSQLStatements()

    val expectationsAsMap = expectations.map(_.asMap())

    val updatedOptions = if (!config.options.contains("SL_TIMEZONE")) {
      val cal1 = Calendar.getInstance
      val tz = cal1.getTimeZone();
      config.options + ("SL_TIMEZONE" -> tz.getID)
    } else {
      config.options
    }
    val updatedConfig = config.copy(options = updatedOptions)
    val depsAsJsonString =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(deps)
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

    new java.util.HashMap[String, Object]() {
      put("config", updatedConfig.asMap)
      put("cron", cron.getOrElse("None"))
      put("dependencies", depsAsJsonString)

      put("statements", statementsAsString)
      put("expectationItems", expectationItemsAsString)
      put("audit", auditAsString)
      put("expectations", expectationsAsString)
      put("acl", aclAsString)
      put("connection", connectionsAsMap)
    }
  }
}
