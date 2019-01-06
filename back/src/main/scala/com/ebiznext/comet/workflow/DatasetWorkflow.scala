package com.ebiznext.comet.workflow

import better.files._
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.{AutoJob, DsvJob, JsonJob}
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.Format.{DSV, JSON}
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Metadata}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.scalatest.path

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
  private def pending(domainName: String): (Iterable[(Option[SchemaModel.Schema], Path)],
    Iterable[(Option[SchemaModel.Schema], Path)]) = {
    val pendingArea = DatasetArea.pending(domainName)
    logger.info(s"List files in $pendingArea")
    val paths = storageHandler.list(pendingArea)
    logger.info(s"Found ${paths.mkString(",")}")
    val domain = schemaHandler.getDomain(domainName).toList
    val schemas: Iterable[(Option[SchemaModel.Schema], Path)] =
      for {
        domain <- domain
        schema <- paths.map { path =>
          (domain.findSchema(path.getName), path) // getName without timestamp
        }
      } yield {
        logger.info(s"Found Schema ${schema._1.map(_.name).getOrElse("None")} for file ${schema._2}")
        schema
      }
    schemas.partition(_._1.isDefined)
  }


  private def ingesting(domain: Domain, schema: SchemaModel.Schema, pendingPath: Path): Unit = {
    val ingestingPath: Path = new Path(DatasetArea.ingesting(domain.name), pendingPath.getName)
    logger.info(s"Start Ingestion on domain: ${domain.name} with schema: ${schema.name} on file: $pendingPath")
    if (storageHandler.move(pendingPath, ingestingPath)) {
      val metadata = domain.metadata.getOrElse(Metadata()).`import`(schema.metadata.getOrElse(Metadata()))
      logger.info(s"Ingesting domain: ${domain.name} with schema: ${schema.name} on file: $pendingPath with metadata $metadata")
      metadata.getFormat() match {
        case DSV =>
          new DsvJob(domain, schema, schemaHandler.types.types, metadata, ingestingPath, storageHandler).run(null)
        case JSON =>
          new JsonJob(domain, schema, schemaHandler.types.types, metadata, ingestingPath, storageHandler).run(null)
      }
      if (Settings.comet.archive) {
        val archivePath = new Path(DatasetArea.archive(domain.name), ingestingPath.getName)
        logger.info(s"Backing up file $ingestingPath to $archivePath")
        storageHandler.move(ingestingPath, archivePath)
      }
      else {
        logger.info(s"Deleting file $ingestingPath")
        storageHandler.delete(ingestingPath)
      }
    }
  }

  def loadPending(includes: List[String] = Nil, excludes: List[String] = Nil): Unit = {
    val domains = (includes, excludes) match {
      case (Nil, Nil) =>
        schemaHandler.domains
      case (_, Nil) =>
        schemaHandler.domains.filter(domain => includes.contains(domain.name))
      case (Nil, _) =>
        schemaHandler.domains.filter(domain => !excludes.contains(domain.name))
    }
    logger.info(s"Domains that will be watched: ${domains.map(_.name).mkString(",")}")

    domains.foreach { domain =>
      logger.info(s"Watch Domain: ${domain.name}")
      val (resolved, unresolved) = pending(domain.name)
      unresolved.foreach {
        case (_, path) =>
          val targetPath = new Path(DatasetArea.unresolved(domain.name), path.getName)
          logger.info(s"Unresolved file : ${path.getName}")
          storageHandler.move(path, targetPath)
      }
      resolved.foreach {
        case (Some(schema), path) =>
          logger.info(s"Ingest resolved file : ${path.getName} with schema ${schema.name}")
          launchHandler.ingest(domain, schema, path)
        case (None, _) => throw new Exception("Should never happen")
      }
    }
  }

  def loadLanding(): Unit = {
    val domains = schemaHandler.domains
    domains.foreach { domain =>
      val inputDir = File(domain.directory)
      logger.info(s"Scanning $inputDir")
      inputDir.list(_.extension.contains(domain.getAck())).foreach { path =>
        val ackFile: File = path
        val fileStr = ackFile.pathAsString
        val prefixStr = fileStr.stripSuffix(domain.getAck())
        val tgz = File(prefixStr + ".tgz")
        val gz = File(prefixStr + ".gz")
        val tmpDir = File(prefixStr)
        val zip = File(prefixStr + ".zip")
        val rawFormats = domain.getExtensions().map(ext => File(prefixStr + ext))
        val existRawFile = rawFormats.find(file => file.exists)
        logger.info(s"Found ack file $ackFile")
        ackFile.delete()
        if (gz.exists) {
          logger.info(s"Found compressed file $gz")
          gz.unGzipTo(tmpDir)
          gz.delete()
        }
        else if (tgz.exists) {
          logger.info(s"Found compressed file $tgz")
          tgz.unGzipTo(tmpDir)
          tgz.delete()
        }
        else if (zip.exists) {
          logger.info(s"Found compressed file $zip")
          zip.unzipTo(tmpDir)
          zip.delete()
        }
        else if (existRawFile.isDefined) {
          existRawFile.foreach { file =>
            logger.info(s"Found raw file $existRawFile")
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

  def ingest(domainName: String, schemaName: String, pendingPath: String): Unit = {
    val domains = schemaHandler.domains
    for {
      domain <- domains.find(_.name == domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield ingesting(domain, schema, new Path(pendingPath))
  }

  def autoJob(jobname: String): Unit = {
    val job = schemaHandler.jobs(jobname)
    job.tasks.foreach { task =>
      val action = new AutoJob(job.name, job.getArea(), task)
      action.run()
    }
  }
}
