package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.utils.Formatter.RichFormatter

import java.util
import scala.jdk.CollectionConverters.seqAsJavaListConverter

// We add

case class DagSchedule(schedule: String, cron: String, domains: java.util.List[DagDomain]) {
  def getSchedule(): String = schedule
  def getDomains(): java.util.List[DagDomain] = domains
}

case class DagDomain(name: String, tables: java.util.List[String]) {
  def getName(): String = name
  def getTables(): java.util.List[String] = tables
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

case class DagGenerationContext(
  config: DagGenerationConfig,
  schedules: List[DagSchedule]
) {
  def asMap: util.HashMap[String, Object] = {
    new java.util.HashMap[String, Object]() {
      put("config", config.asMap)
      put("schedules", schedules.asJava)
    }
  }
}
