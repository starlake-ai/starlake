package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.schema.generator.TaskViewDependencyNode
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.JsonSerializer

import java.util
import java.util.Calendar
import scala.jdk.CollectionConverters.seqAsJavaListConverter

// We add

case class DagSchedule(schedule: String, cron: String, domains: java.util.List[DagDomain]) {
  def getSchedule(): String = schedule

  def getCron(): String = cron

  def getDomains(): java.util.List[DagDomain] = domains
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

case class DagGenerationConfig(
  comment: String,
  template: String,
  filename: String,
  options: Map[String, String]
) {
  def getfilenameVars()(implicit settings: Settings): Set[String] = filename.extractVars()

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

case class LoadDagGenerationContext(
  config: DagGenerationConfig,
  schedules: List[DagSchedule]
) {
  def asMap: util.HashMap[String, Object] = {
    val updatedOptions = if (!config.options.contains("SL_TIMEZONE")) {
      val cal1 = Calendar.getInstance
      val tz = cal1.getTimeZone();
      config.options + ("SL_TIMEZONE" -> tz.getID)
    } else {
      config.options
    }
    val updatedConfig = config.copy(options = updatedOptions)
    new java.util.HashMap[String, Object]() {
      put("config", updatedConfig.asMap)
      put("schedules", schedules.asJava)
    }
  }
}
case class TransformDagGenerationContext(
  config: DagGenerationConfig,
  deps: List[TaskViewDependencyNode],
  cron: Option[String]
) {
  def asMap: util.HashMap[String, Object] = {
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

    new java.util.HashMap[String, Object]() {
      put("config", updatedConfig.asMap)
      put("cron", cron.getOrElse("None"))
      put("dependencies", depsAsJsonString)
    }
  }
}
