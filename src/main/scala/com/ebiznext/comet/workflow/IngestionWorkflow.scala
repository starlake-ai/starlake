/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.ebiznext.comet.workflow

import better.files._
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.index.{IndexConfig, IndexJob}
import com.ebiznext.comet.job.ingest.{DsvIngestionJob, JsonIngestionJob, SimpleJsonIngestionJob}
import com.ebiznext.comet.job.transform.AutoJob
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Format.{DSV, JSON, SIMPLE_JSON}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

/**
  * The whole worklfow works as follow :
  *   - loadLanding : Zipped files are uncompressed or raw files extracted from the local filesystem.
  * -loadPending :
  * files recognized with filename patterns are stored in the ingesting area and submitted for ingestion
  * files with unrecognized filename patterns are stored in the unresolved area
  *   - ingest : files are finally ingested and saved as parquet/orc/... files and hive tables
  *
  * @param storageHandler : Minimum set of features required for the underlying filesystem
  * @param schemaHandler  : Schema interface
  * @param launchHandler  : Cron Manager interface
  */
class IngestionWorkflow(
                         storageHandler: StorageHandler,
                         schemaHandler: SchemaHandler,
                         launchHandler: LaunchHandler
                       ) extends StrictLogging {
  val domains: List[Domain] = schemaHandler.domains

  /**
    * Load file from the landing area
    * files are loaded one domain at a time
    * each domain has its own directory
    * compressed files are uncompressed if a corresponding ack file exist.
    * raw file should also have a corresponding ack file
    * before moving the files to the pending area, the ack files are deleted
    */
  def loadLanding(): Unit = {
    domains.foreach { domain =>
      val inputDir = File(domain.directory)
      logger.info(s"Scanning $inputDir")
      inputDir.list(_.extension.contains(domain.getAck())).toList.foreach { path =>
        val ackFile: File = path
        val fileStr = ackFile.pathAsString
        val prefixStr = fileStr.stripSuffix(domain.getAck())
        val tgz = File(prefixStr + ".tgz")
        val gz = File(prefixStr + ".gz")
        val tmpDir = File(prefixStr)
        val zip = File(prefixStr + ".zip")
        val rawFormats =
          domain.getExtensions().map(ext => File(prefixStr + ext))
        val existRawFile = rawFormats.find(file => file.exists)
        logger.info(s"Found ack file $ackFile")
        ackFile.delete()
        if (gz.exists) {
          logger.info(s"Found compressed file $gz")
          gz.unGzipTo(tmpDir)
          gz.delete()
        } else if (tgz.exists) {
          logger.info(s"Found compressed file $tgz")
          tgz.unGzipTo(tmpDir)
          tgz.delete()
        } else if (zip.exists) {
          logger.info(s"Found compressed file $zip")
          zip.unzipTo(tmpDir)
          zip.delete()
        } else if (existRawFile.isDefined) {
          existRawFile.foreach { file =>
            logger.info(s"Found raw file $existRawFile")
            tmpDir.createDirectories()
            val tmpFile = File(tmpDir, file.name)
            file.moveTo(tmpFile)
          }
        } else {
          logger.error(s"No archive found for file ${ackFile.pathAsString}")
        }
        if (tmpDir.exists) {
          val destFolder = DatasetArea.pending(domain.name)
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

  /**
    * Split files into resolved and unresolved datasets. A file is unresolved if a corresponding schema is not found.
    * Schema matching is based on the dataset filename pattern
    *
    * @param includes Load pending dataset of these domain only
    * @param excludes : Do not load datasets of these domains
    *                 if both lists are empty, all domains are included
    */
  def loadPending(includes: List[String] = Nil, excludes: List[String] = Nil): Unit = {
    val includedDomains = (includes, excludes) match {
      case (Nil, Nil) =>
        domains
      case (_, Nil) =>
        domains.filter(domain => includes.contains(domain.name))
      case (Nil, _) =>
        domains.filter(domain => !excludes.contains(domain.name))
      case (_, _) => throw new Exception("Should never happen ")
    }
    logger.info(s"Domains that will be watched: ${domains.map(_.name).mkString(",")}")

    includedDomains.foreach { domain =>
      logger.info(s"Watch Domain: ${domain.name}")
      val (resolved, unresolved) = pending(domain.name)
      unresolved.foreach {
        case (_, path) =>
          val targetPath =
            new Path(DatasetArea.unresolved(domain.name), path.getName)
          logger.info(s"Unresolved file : ${path.getName}")
          storageHandler.move(path, targetPath)
      }
      resolved.foreach {
        case (Some(schema), pendingPath) =>
          logger.info(s"Ingest resolved file : ${pendingPath.getName} with schema ${schema.name}")
          val ingestingPath: Path =
            new Path(DatasetArea.ingesting(domain.name), pendingPath.getName)
          if (storageHandler.move(pendingPath, ingestingPath))
            launchHandler.ingest(this, domain, schema, ingestingPath)

        case (None, _) => throw new Exception("Should never happen")
      }
    }
  }

  /**
    *
    * @param domainName : Domaine name
    * @return resolved && unresolved schemas / path
    */
  private def pending(
                       domainName: String
                     ): (Iterable[(Option[Schema], Path)], Iterable[(Option[Schema], Path)]) = {
    val pendingArea = DatasetArea.pending(domainName)
    logger.info(s"List files in $pendingArea")
    val paths = storageHandler.list(pendingArea)
    logger.info(s"Found ${paths.mkString(",")}")
    val domain = schemaHandler.getDomain(domainName).toList
    val schemas: Iterable[(Option[Schema], Path)] =
      for {
        domain <- domain
        schema <- paths.map { path =>
          (domain.findSchema(path.getName), path) // getName without timestamp
        }
      } yield {
        logger.info(
          s"Found Schema ${schema._1.map(_.name).getOrElse("None")} for file ${schema._2}"
        )
        schema
      }
    schemas.partition(_._1.isDefined)
  }

  /**
    * Ingest the file (called by the cron manager at ingestion time for a specific dataset
    *
    * @param domainName    : domain name of the dataset
    * @param schemaName    schema name of the dataset
    * @param ingestingPath : Absolute path of the file to ingest (present in the ingesting area of the domain)
    */
  def ingest(domainName: String, schemaName: String, ingestingPath: String): Unit = {
    for {
      domain <- domains.find(_.name == domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield ingesting(domain, schema, new Path(ingestingPath))
    ()
  }

  private def ingesting(domain: Domain, schema: Schema, ingestingPath: Path): Unit = {
    logger.info(
      s"Start Ingestion on domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath"
    )
    val metadata = domain.metadata
      .getOrElse(Metadata())
      .`import`(schema.metadata.getOrElse(Metadata()))
    logger.info(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath with metadata $metadata"
    )
    val ingestionResult = Try(metadata.getFormat() match {
      case DSV =>
        new DsvIngestionJob(domain, schema, schemaHandler.types, ingestingPath, storageHandler)
          .run()
      case SIMPLE_JSON =>
        new SimpleJsonIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler
        ).run()
      case JSON =>
        new JsonIngestionJob(domain, schema, schemaHandler.types, ingestingPath, storageHandler)
          .run()
      case _ =>
        throw new Exception("Should never happen")
    })
    ingestionResult match {
      case Success(_) =>
        if (Settings.comet.archive) {
          val archivePath =
            new Path(DatasetArea.archive(domain.name), ingestingPath.getName)
          logger.info(s"Backing up file $ingestingPath to $archivePath")
          val _ = storageHandler.move(ingestingPath, archivePath)
        } else {
          logger.info(s"Deleting file $ingestingPath")
          val _ = storageHandler.delete(ingestingPath)
        }
      case Failure(exception) =>
        Utils.logException(logger, exception)
    }

    val meta = schema.mergedMetadata(domain.metadata)
    if (meta.isIndexed() && Settings.comet.elasticsearch.active) {
      val mapping = schema.mergedMetadata(domain.metadata).mapping
      index(IndexConfig(
        resource = mapping.flatMap(_.resource),
        id = mapping.flatMap(_.id),
        format = "parquet",
        domain = domain.name,
        schema = schema.name))
    }
  }

  def index(job: AutoJobDesc, task: AutoTask): Unit = {
    val targetArea = task.area.getOrElse(job.getArea())
    val targetPath = new Path(DatasetArea.path(task.domain, targetArea.value), task.dataset)
    val mapping = task.mapping
    index(IndexConfig(
      resource = mapping.flatMap(_.resource),
      id = mapping.flatMap(_.id),
      format = "parquet",
      domain = task.domain,
      schema = task.dataset,
      dataset = Some(targetPath)))
  }

  /**
    * Successively run each task of a job
    *
    * @param jobname : job namle as defined in the YML file.
    */
  def autoJob(jobname: String): Unit = {
    val job = schemaHandler.jobs(jobname)
    job.tasks.foreach { task =>
      val action = new AutoJob(job.name, job.getArea(), task)
      action.run()
      if (task.isIndexed() && Settings.comet.elasticsearch.active) {
        index(job, task)
      }
    }
  }

  /**
    * Successively run each task of a job
    *
    * @param job : job as defined in the YML file.
    */
  def autoJob(job: AutoJobDesc): Unit = {
    job.tasks.foreach { task =>
      val action = new AutoJob(job.name, job.getArea(), task)
      action.run()
      index(job, task)
    }
  }

  def index(config: IndexConfig) = {
    new IndexJob(config, Settings.storageHandler).run()
  }
}
