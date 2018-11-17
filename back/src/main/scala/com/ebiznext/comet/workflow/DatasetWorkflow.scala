package com.ebiznext.comet.workflow

import better.files
import better.files._
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.job.DsvJob
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.Domain
import com.ebiznext.comet.schema.model.SchemaModel.Format.{DSV, JSON}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{LocalFileSystem, Path}

class DatasetWorkflow(storageHandler: StorageHandler,
                      schemaHandler: SchemaHandler,
                      launchHandler: LaunchHandler) extends StrictLogging {

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
    val schemas: Iterable[(Option[SchemaModel.Schema], Path)] = for {
      schema <- paths.map { path =>
        (domain.get.findSchema(path.getName), path)
      }
    } yield schema
    schemas.partition(_._1.isDefined)
  }


  private def staging(domain: Domain, schema: SchemaModel.Schema, path: Path): Unit = {
    val metadata = domain.metadata.merge(schema.metadata)

    metadata.getFormat() match {
      case DSV =>
        new DsvJob(domain, schema, schemaHandler.types.types, metadata, path, storageHandler).run(null)
      case JSON =>
        throw new Exception("Not yet Implemented");
    }
    val targetPath = new Path(DatasetArea.staging(domain.name), path.getName)
    storageHandler.move(path, targetPath)
  }

  private def ingesting(domain: Domain, schema: SchemaModel.Schema, path: Path): Unit = {
    val targetPath = new Path(DatasetArea.ingesting(domain.name), path.getName)
    if (storageHandler.move(path, targetPath)) {
      staging(domain, schema, targetPath)
    }
  }

  def loadPending(): Unit = {
    val domains = schemaHandler.domains
    domains.foreach { domain =>
      val (resolved, unresolved) = pending(domain.name)
      unresolved.foreach {
        case (_, path) =>
          val targetPath = new Path(DatasetArea.unresolved(domain.name), path.getName)
          storageHandler.move(path, targetPath)
      }
      resolved.foreach {
        case (Some(schema), path) =>
          //ingesting(domain, schema, path)
          launchHandler.ingest(domain.name, schema.name, path)
        case (None, _) => throw new Exception("Should never happen")
      }
    }
  }

  def loadLanding(): Unit = {
    val localFS = new LocalFileSystem
    val domains = schemaHandler.domains
    domains.foreach { domain =>
      storageHandler.list(new Path(domain.directory), ".ack").foreach { path: Path =>
        val ackFile: files.File = localFS.pathToFile(path).toScala
        val fileStr = ackFile.pathAsString
        val prefixStr = fileStr.stripSuffix(".ack")
        val tgz = File(prefixStr + ".tgz")
        val tmpDir = File(prefixStr)
        val zip = File(prefixStr + ".zip")
        ackFile.delete()
        if (tgz.exists) {
          tgz.unGzipTo(tmpDir)
        }
        else if (zip.exists) {
          tgz.unzipTo(tmpDir)
        }
        else {
          logger.error(s"No archive found for file ${ackFile.pathAsString}")
        }
        if (tmpDir.exists) {
          val dest = DatasetArea.pending(domain.name)
          tmpDir.list.foreach { file =>
            val source = new Path(file.pathAsString)
            storageHandler.moveFromLocal(source, dest)
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
}
