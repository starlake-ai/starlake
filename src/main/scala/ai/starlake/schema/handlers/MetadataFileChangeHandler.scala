package ai.starlake.schema.handlers

import ai.starlake.config.{DatasetArea, Settings}
import better.files.File
import com.typesafe.scalalogging.StrictLogging

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

private class MetadataFileChangeHandler(schemaHandler: SchemaHandler)(implicit settings: Settings)
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
        case "load" :: domain :: schema :: Nil if schema.toLowerCase().matches("^[a-z].*$") =>
          onTableEvent(eventType, domain, schema, file)
        case "transform" :: domain :: "_config" :: Nil =>
          onTransformDomainEvent(eventType, domain, file)
        case "transform" :: domain :: task :: Nil if task.toLowerCase().matches("^[a-z].*$") =>
          onTaskEvent(eventType, domain, task, file)
        case _ =>
        // ignore
      }
    }
  }

  private var watchService: WatchService = null
  private val keys: mutable.Map[WatchKey, Path] = new mutable.HashMap[WatchKey, Path]()
  private var listening = false

  @throws[IOException]
  private def registerRecursive(root: Path): Unit = {
    // register all subfolders
    Files.walkFileTree(
      root,
      new SimpleFileVisitor[Path]() {
        @throws[IOException]
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val key = dir.register(
            watchService,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE,
            StandardWatchEventKinds.ENTRY_MODIFY
          )
          keys.put(key, dir);
          FileVisitResult.CONTINUE
        }
      }
    )
  }

  def processEvents(): Unit = {
    while (listening) {
      val key =
        try {
          watchService.take
        } catch {
          case _: ClosedWatchServiceException =>
            logger.info("WatchService closed")
            return
          case _: InterruptedException =>
            logger.info("WatchService interrupted")
            return
        }
      for (event <- key.pollEvents.asScala) {
        try {
          val kind = event.asInstanceOf[WatchEvent[Path]].kind
          if (kind != StandardWatchEventKinds.OVERFLOW) {
            val ev = event.asInstanceOf[WatchEvent[Path]]
            val name = ev.context
            val dir = keys.get(key)
            dir.foreach { dir =>
              val child =
                try {
                  dir.resolve(name)
                } catch {
                  case e: Exception =>
                    logger.error("Error resolving path", e)
                    Path.of(dir.toUri.getPath, name.toString)
                }
              onEvent(kind, File(child), event.count)
              if (
                kind == StandardWatchEventKinds.ENTRY_CREATE &&
                Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)
              ) {
                registerRecursive(child);
              }
            }
          } else {
            logger.warn("WatchKey not recognized!!")
          }
          val valid = key.reset
          if (!valid) {
            keys.remove(key)
          }
        } catch {
          case e: Exception =>
            logger.error("Error processing event", e)
        }
      }
    }
  }

  def start() = {
    val pathToWatch = Path.of(DatasetArea.metadata(settings).toUri.getPath)
    watchService = pathToWatch.getFileSystem.newWatchService
    global.execute { () =>
      registerRecursive(pathToWatch)
      listening = true
      processEvents()
    }
  }

  def stop(): Unit = {
    listening = false
    watchService.close()
  }
}

object MetadataFileChangeHandler extends StrictLogging {
  private var handler: MetadataFileChangeHandler = null
  private var listen: Boolean = false

  def isListening(): Boolean = listen
  def startListening(): Unit = listen = true
  def stopListening(): Unit = listen = false

  def start(schemaHandler: SchemaHandler)(implicit settings: Settings): Unit = {
    if (isListening()) {
      if (handler != null) {
        stop()
      }
      handler = new MetadataFileChangeHandler(schemaHandler)
      handler.start()
    }
  }

  def stop(): Unit = {
    if (isListening()) {
      handler.stop()
      handler = null
    }
  }
}
