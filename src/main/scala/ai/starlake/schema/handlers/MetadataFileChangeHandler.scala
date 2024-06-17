package ai.starlake.schema.handlers

import ai.starlake.config.{DatasetArea, Settings}
import better.files.File
import com.typesafe.scalalogging.StrictLogging

import java.nio.file.{Path, StandardWatchEventKinds, WatchEvent}

class MetadataFileChangeHandler(schemaHandler: SchemaHandler)(implicit settings: Settings)
    extends StrictLogging {

  def onLoadDomainEvent(eventType: WatchEvent.Kind[Path], domain: String, file: File): Unit = {
    eventType match {
      case StandardWatchEventKinds.ENTRY_CREATE =>
        logger.info(s"File watcher noticed config file for domain $domain created.")
      case StandardWatchEventKinds.ENTRY_MODIFY =>
        logger.info(s"File watcher noticed config file for domain $domain modified.")
      case StandardWatchEventKinds.ENTRY_DELETE =>
        logger.info(s"File watcher noticed config file for domain $domain deleted.")
      case _ =>
        println(s"Unknown event type: $eventType")
    }
  }

  def onTransformDomainEvent(eventType: WatchEvent.Kind[Path], domain: String, file: File): Unit = {
    eventType match {
      case StandardWatchEventKinds.ENTRY_CREATE =>
        logger.info(s"File watcher noticed config file for domain $domain created.")
      case StandardWatchEventKinds.ENTRY_MODIFY =>
        logger.info(s"File watcher noticed config file for domain $domain modified.")
      case StandardWatchEventKinds.ENTRY_DELETE =>
        logger.info(s"File watcher noticed config file for domain $domain deleted.")
      case _ =>
        println(s"Unknown event type: $eventType")
    }
  }

  def onTableEvent(
    eventType: WatchEvent.Kind[Path],
    domain: String,
    table: String,
    file: File
  ): Unit = {
    eventType match {
      case StandardWatchEventKinds.ENTRY_CREATE =>
      // onTableEventCreate(domain, table, file)
      case StandardWatchEventKinds.ENTRY_MODIFY =>
        schemaHandler.onTableChange(domain, table, file)
      case StandardWatchEventKinds.ENTRY_DELETE =>
        schemaHandler.onTableDelete(domain, table)
      case _ =>
        println(s"Unknown event type: $eventType")
    }
  }

  def onTaskEvent(
    eventType: WatchEvent.Kind[Path],
    domain: String,
    task: String,
    file: File
  ): Unit = {
    eventType match {
      case StandardWatchEventKinds.ENTRY_CREATE =>
      // onTaskEventCreate(domain, task, file)
      case StandardWatchEventKinds.ENTRY_MODIFY =>
        schemaHandler.onTaskChange(domain, task, file)
      case StandardWatchEventKinds.ENTRY_DELETE =>
        schemaHandler.onTaskDelete(domain, task)
      case _ =>
        println(s"Unknown event type: $eventType")
    }
  }

  def onEvent(eventType: WatchEvent.Kind[Path], file: File, count: Int): Unit = {
    val prefix = DatasetArea.metadata.toString() + "/"
    val f = file.toString().substring(prefix.length())
    if (f.endsWith(".sl.yml")) {
      val fWithoutSuffix = f.substring(0, f.length - ".sl.yml".length)
      fWithoutSuffix.split("/").toList match {
        case "load" :: domain :: "_config" :: Nil =>
          onLoadDomainEvent(eventType, domain, file)
        case "load" :: domain :: schema :: Nil
            if schema.toLowerCase().matches("^[a-z].*\\.sl\\.yml$") =>
          onTableEvent(eventType, domain, schema, file)
        case "transform" :: domain :: "_config" :: Nil =>
          onTransformDomainEvent(eventType, domain, file)
        case "transform" :: domain :: task :: Nil
            if task.toLowerCase().matches("^[a-z].*\\.sl\\.yml$") =>
          onTaskEvent(eventType, domain, task, file)
        case _ =>
        // ignore
      }
    }
  }
}
