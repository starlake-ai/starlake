package com.ebiznext.comet.workflow

import better.files._
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.{AutoBusinessJob, DsvJob, JsonJob}
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.Format.{DSV, JSON}
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Metadata}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

class DatasetWorkflow(storageHandler: StorageHandler,
                      schemaHandler: SchemaHandler,
                      launchHandler: LaunchHandler) extends StrictLogging {

  //  private val timeFormat = "yyyyMMdd-HHmmss-SSS"
  //  private val timePattern = Pattern.compile(".+\\.\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d-\\d\\d\\d")
  //
  //  private def nameWithNowTime(name: String): String = {
  //    name + "." + LocalDateTime.now().format(DateTimeFormatter.ofPattern(timeFormat))
  //  }
  //
  //  private def nameWithoutNowTime(name: String): String = {
  //    if (timePattern.matcher(name).matches()) {
  //      name.substring(0, name.lastIndexOf('.'))
  //    }
  //    else
  //      name
  //  }

  /**
    *
    * @param domainName
    * @return resolved && unresolved schemas / path
    */
  private def pending(
                       domainName: String): (Iterable[(Option[SchemaModel.Schema], Path)],
    Iterable[(Option[SchemaModel.Schema], Path)]) = {
    val paths = storageHandler.list(DatasetArea.pending(domainName))
    val domain = schemaHandler.getDomain(domainName)
    val schemas: Iterable[(Option[SchemaModel.Schema], Path)] =
      for {
        schema <- paths.map { path =>
          (domain.get.findSchema(path.getName), path) // getName without timestamp
        }
      } yield schema
    schemas.partition(_._1.isDefined)
  }


  private def staging(domain: Domain, schema: SchemaModel.Schema, path: Path): Unit = {
    val metadata = domain.metadata.getOrElse(Metadata()).`import`(schema.metadata.getOrElse(Metadata()))

    metadata.getFormat() match {
      case DSV =>
        new DsvJob(domain, schema, schemaHandler.types.types, metadata, path, storageHandler).run(null)
      case JSON =>
        new JsonJob(domain, schema, schemaHandler.types.types, metadata, path, storageHandler).run(null)
    }
    if (Settings.comet.staging) {
      val targetPath = new Path(DatasetArea.staging(domain.name), path.getName)
      storageHandler.move(path, targetPath)
    }
    else {
      storageHandler.delete(path)
    }
  }

  private def ingesting(domain: Domain, schema: SchemaModel.Schema, path: Path): Unit = {
    val targetPath = new Path(DatasetArea.ingesting(domain.name), path.getName)
    if (storageHandler.move(path, targetPath)) {
      staging(domain, schema, targetPath)
    }
  }

  def loadPending(includes: List[String] = Nil, excludes: List[String] = Nil): Unit = {
    val domains = (includes, excludes) match {
      case (Nil, Nil) => schemaHandler.domains
      case (_, Nil) =>
        schemaHandler.domains.filter(domain => includes.contains(domain.name))
      case (Nil, _) =>
        schemaHandler.domains.filter(domain => !excludes.contains(domain.name))
    }

    domains.foreach { domain =>
      val (resolved, unresolved) = pending(domain.name)
      unresolved.foreach {
        case (_, path) =>
          val targetPath = new Path(DatasetArea.unresolved(domain.name), path.getName)
          storageHandler.move(path, targetPath)
      }
      resolved.foreach {
        case (Some(schema), path) =>
          launchHandler.ingest(domain, schema, path)
        case (None, _) => throw new Exception("Should never happen")
      }
    }
  }

  def loadLanding(): Unit = {
    val domains = schemaHandler.domains
    domains.foreach { domain =>
      val inputDir = File(domain.directory)

      inputDir.list(_.extension.contains(".ack")).foreach { path =>
        val ackFile: File = path
        val fileStr = ackFile.pathAsString
        val prefixStr = fileStr.stripSuffix(".ack")
        val tgz = File(prefixStr + ".tgz")
        val gz = File(prefixStr + ".gz")
        val tmpDir = File(prefixStr)
        val zip = File(prefixStr + ".zip")
        val rawFormats = Array(".json", ".csv", ".dsv", ".psv").map(ext => File(prefixStr + ext))
        val existRawFile = rawFormats.find(file => file.exists)
        ackFile.delete()
        if (gz.exists) {
          gz.unGzipTo(tmpDir)
          gz.delete()
        }
        else if (tgz.exists) {
          tgz.unGzipTo(tmpDir)
          tgz.delete()
        }
        else if (zip.exists) {
          zip.unzipTo(tmpDir)
          zip.delete()
        }
        else if (existRawFile.isDefined) {
          existRawFile.foreach { file =>
            val tmpFile = File(tmpDir, file.name)
            tmpDir.createDirectories()
            file.moveTo(tmpFile)
          }
        }
        else {
          logger.error(s"No archive found for file ${ackFile.pathAsString}")
        }
        if (tmpDir.exists) {
          val destFolder = DatasetArea.pending(domain.name) // Add FileName with timestamp in nanos
          tmpDir.list.foreach { file =>
            val source = new Path(file.pathAsString)
            logger.info(s"Importing ${file.pathAsString}")
            val destFile = new Path(destFolder, file.name)
            storageHandler.moveFromLocal(source, destFile)
          }
          tmpDir.delete()
        }
      }
    }
  }

  def ingest(domainName: String, schemaName: String, path: String): Unit = {
    val domains = schemaHandler.domains
    for {
      domain <- domains.find(_.name == domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield ingesting(domain, schema, new Path(path))
  }

  def businessJob(jobname: String): Unit = {
    val job = schemaHandler.business(jobname)
    job.tasks.foreach { task =>
      val action = new AutoBusinessJob(job.name, task)
      action.run()
    }
  }
}
