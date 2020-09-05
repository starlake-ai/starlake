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

import better.files.File
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.atlas.{AtlasConfig, AtlasJob}
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQuerySparkJob}
import com.ebiznext.comet.job.index.esload.{ESLoadConfig, ESLoadJob}
import com.ebiznext.comet.job.index.jdbcload.{JdbcLoadConfig, JdbcLoadJob}
import com.ebiznext.comet.job.infer.{InferSchema, InferSchemaConfig}
import com.ebiznext.comet.job.ingest._
import com.ebiznext.comet.job.metrics.{MetricsConfig, MetricsJob}
import com.ebiznext.comet.job.transform.AutoTaskJob
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Format._
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.{Unpacker, Utils}
import com.google.cloud.bigquery.{Schema => BQSchema}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

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
)(implicit settings: Settings)
    extends StrictLogging {
  val domains: List[Domain] = schemaHandler.domains

  /**
    * Move the files from the landing area to the pending area.
    * files are loaded one domain at a time
    * each domain has its own directory and is specified in the "directory" key of Domain YML file
    * compressed files are uncompressed if a corresponding ack file exist.
    * Compressed files are recognized by their extension which should be one of .tgz, .zip, .gz.
    * raw file should also have a corresponding ack file
    * before moving the files to the pending area, the ack files are deleted
    * To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML file.
    * "ack" is the default ack extension searched for but you may specify a different one in the domain YML file.
    */
  def loadLanding(): Unit = {
    logger.info("LoadLanding")
    domains.foreach { domain =>
      val storageHandler = settings.storageHandler
      val inputDir = new Path(domain.directory)
      logger.info(s"Scanning $inputDir")
      storageHandler.list(inputDir, domain.getAck()).foreach { path =>
        val ackFile = path
        val fileStr = ackFile.toString
        val prefixStr =
          if (domain.getAck().isEmpty) fileStr.substring(0, fileStr.lastIndexOf('.'))
          else fileStr.stripSuffix(domain.getAck())
        val tmpDir = new Path(prefixStr)
        val rawFormats =
          if (domain.getAck().isEmpty) List(ackFile)
          else
            domain.getExtensions().map(ext => new Path(prefixStr + ext))
        val existRawFile = rawFormats.find(file => storageHandler.exists(file))
        logger.info(s"Found ack file $ackFile")
        if (domain.getAck().nonEmpty)
          storageHandler.delete(ackFile)
        if (existRawFile.isDefined) {
          existRawFile.foreach { file =>
            logger.info(s"Found raw file $existRawFile")
            storageHandler.mkdirs(tmpDir)
            val tmpFile = new Path(tmpDir, file.getName)
            storageHandler.move(file, tmpFile)
          }
        } else if (storageHandler.fs.getScheme == "file") {
          storageHandler.mkdirs(tmpDir)
          val tgz = new Path(prefixStr + ".tgz")
          val gz = new Path(prefixStr + ".gz")
          val zip = new Path(prefixStr + ".zip")
          val tmpFile = Path.getPathWithoutSchemeAndAuthority(tmpDir).toString
          if (storageHandler.exists(gz)) {
            logger.info(s"Found compressed file $gz")

            File(Path.getPathWithoutSchemeAndAuthority(gz).toString)
              .unGzipTo(File(tmpFile, File(prefixStr).name))
            storageHandler.delete(gz)
          } else if (storageHandler.exists(tgz)) {
            logger.info(s"Found compressed file $tgz")
            Unpacker
              .unpack(File(Path.getPathWithoutSchemeAndAuthority(tgz).toString), File(tmpFile))
            storageHandler.delete(tgz)
          } else if (storageHandler.exists(zip)) {
            logger.info(s"Found compressed file $zip")
            File(Path.getPathWithoutSchemeAndAuthority(zip).toString)
              .unzipTo(File(tmpFile))
            storageHandler.delete(zip)
          } else {
            logger.error(s"No archive found for ack ${ackFile.toString}")
          }
        } else {
          logger.error(s"No file found for ack ${ackFile.toString}")
        }
        if (storageHandler.exists(tmpDir)) {
          val destFolder = DatasetArea.pending(domain.name)
          storageHandler.list(tmpDir).foreach { file =>
            val source = new Path(file.toString)
            logger.info(s"Importing ${file.toString}")
            val destFile = new Path(destFolder, file.getName)
            storageHandler.moveFromLocal(source, destFile)
          }
          storageHandler.delete(tmpDir)
        }
      }
    }
  }

  /**
    * Split files into resolved and unresolved datasets. A file is unresolved
    * if a corresponding schema is not found.
    * Schema matching is based on the dataset filename pattern
    *
    * @param config : includes Load pending dataset of these domain only
    *                 excludes : Do not load datasets of these domains
    *                 if both lists are empty, all domains are included
    */
  def loadPending(config: WatchConfig = WatchConfig()): Unit = {
    val includedDomains = (config.includes, config.excludes) match {
      case (Nil, Nil) =>
        domains
      case (_, Nil) =>
        domains.filter(domain => config.includes.contains(domain.name))
      case (Nil, _) =>
        domains.filter(domain => !config.excludes.contains(domain.name))
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

      // We group files with the same schema to ingest them together in a single step.
      val groupedResolved: Map[Schema, Iterable[Path]] = resolved.map {
          case (Some(schema), path) => (schema, path)
          case (None, _)            => throw new Exception("Should never happen")
        } groupBy (_._1) mapValues (it => it.map(_._2))

      groupedResolved.foreach {
        case (schema, pendingPaths) =>
          logger.info(s"""Ingest resolved file : ${pendingPaths
            .map(_.getName)
            .mkString(",")} with schema ${schema.name}""")
          val ingestingPaths = pendingPaths.map { pendingPath =>
            val ingestingPath = new Path(DatasetArea.ingesting(domain.name), pendingPath.getName)
            if (!storageHandler.move(pendingPath, ingestingPath)) {
              logger.error(s"Could not move $pendingPath to $ingestingPath")
            }
            ingestingPath
          }
          try {
            if (settings.comet.grouped)
              launchHandler.ingest(this, domain, schema, ingestingPaths.toList)
            else
              ingestingPaths.foreach(launchHandler.ingest(this, domain, schema, _))
          } catch {
            case t: Throwable =>
              t.printStackTrace()
            // Continue to next pending file
          }
      }
    }
  }

  /**
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
    */
  def ingest(config: LoadConfig): Unit = {
    val domainName = config.domain
    val schemaName = config.schema
    val ingestingPaths = config.paths
    for {
      domain <- domains.find(_.name == domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield ingesting(domain, schema, ingestingPaths)
    ()
  }

  private def ingesting(domain: Domain, schema: Schema, ingestingPath: List[Path]): Unit = {
    logger.info(
      s"Start Ingestion on domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath"
    )
    val metadata = domain.metadata
      .getOrElse(Metadata())
      .`import`(schema.metadata.getOrElse(Metadata()))
    logger.info(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath with metadata $metadata"
    )
    val ingestionResult: Try[Unit] = Try(metadata.getFormat() match {
      case DSV =>
        new DsvIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler
        ).run().get
      case SIMPLE_JSON =>
        new SimpleJsonIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler
        ).run().get
      case JSON =>
        new JsonIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler
        ).run().get
      case POSITION =>
        new PositionIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler
        ).run().get
      case CHEW =>
        ChewerJob
          .run(
            s"${settings.comet.chewerPrefix}.${domain.name}.${schema.name}",
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler
          )
          .get

      case _ =>
        throw new Exception("Should never happen")
    })
    ingestionResult match {
      case Success(_) =>
        if (settings.comet.archive) {
          ingestingPath.foreach { ingestingPath =>
            val archivePath =
              new Path(DatasetArea.archive(domain.name), ingestingPath.getName)
            logger.info(s"Backing up file $ingestingPath to $archivePath")
            val _ = storageHandler.move(ingestingPath, archivePath)
          }
        } else {
          logger.info(s"Deleting file $ingestingPath")
          ingestingPath.foreach(storageHandler.delete)
        }
      case Failure(exception) =>
        Utils.logException(logger, exception)
    }
  }

  def esload(job: AutoJobDesc, task: AutoTaskDesc): Unit = {
    val targetArea = task.area.getOrElse(job.getArea())
    val targetPath = new Path(DatasetArea.path(task.domain, targetArea.value), task.dataset)
    val sink = task.getSink().asInstanceOf[EsSink]
    launchHandler.esLoad(
      this,
      ESLoadConfig(
        timestamp = sink.timestamp,
        id = sink.id,
        format = "parquet",
        domain = task.domain,
        schema = task.dataset,
        dataset = Some(targetPath)
      )
    )
  }

  def infer(config: InferSchemaConfig): InferSchema = {
    new InferSchema(
      config.domainName,
      config.schemaName,
      config.inputPath,
      config.outputPath,
      config.header
    )
  }

  /**
    * Successively run each task of a job
    *
    * @param config : job name as defined in the YML file and sql parameters to pass to SQL statements.
    */
  def autoJob(config: TransformConfig): Unit = {
    val job = schemaHandler.jobs(config.name)
    job.tasks.foreach { task =>
      val action = new AutoTaskJob(
        job.name,
        job.area,
        job.format,
        job.coalesce.getOrElse(false),
        job.udf,
        job.views,
        job.getEngine(),
        task,
        storageHandler,
        config.options
      )
      val (createDisposition, writeDisposition) =
        Utils.getDBDisposition(task.write, hasMergeKeyDefined = false)
      job.getEngine() match {
        case Engine.BQ =>
          action.runBQ()
          task.sink.map(sink => sink.asInstanceOf[BigQuerySink]).foreach { bqSink =>
            bqload(
              BigQueryLoadConfig(
                outputTable = task.dataset,
                outputDataset = task.domain,
                createDisposition = createDisposition,
                writeDisposition = writeDisposition,
                location = bqSink.location,
                outputPartition = bqSink.timestamp,
                outputClustering = bqSink.clustering.getOrElse(Nil),
                days = bqSink.days,
                requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
                rls = task.rls,
                engine = Engine.BQ
              )
            )
          }

        case Engine.SPARK =>
          action.runSpark() match {
            case Success(maybeDataFrame) =>
              task.getSink() match {
                case Some(sink)
                    if settings.comet.elasticsearch.active && sink.`type` == SinkType.ES =>
                  esload(job, task)
                case Some(sink) if sink.`type` == SinkType.BQ =>
                  val bqSink = sink.asInstanceOf[BigQuerySink]
                  val source = maybeDataFrame
                    .map(df => Right(setNullableStateOfColumn(df, nullable = true)))
                    .getOrElse(Left(task.getTargetPath(Some(job.getArea())).toString))
                  val config =
                    BigQueryLoadConfig(
                      source = source,
                      outputTable = task.dataset,
                      outputDataset = task.domain,
                      sourceFormat = "parquet",
                      createDisposition = createDisposition,
                      writeDisposition = writeDisposition,
                      location = bqSink.location,
                      outputPartition = bqSink.timestamp,
                      outputClustering = bqSink.clustering.getOrElse(Nil),
                      days = bqSink.days,
                      requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
                      rls = task.rls
                    )
                  new BigQuerySparkJob(config, None).run()
                case _ =>
                // ignore

              }
            case Failure(exception) =>
              exception.printStackTrace()
          }
        case _ => Failure(new Exception("Should never happen"))
      }
    }
  }

  def esLoad(config: ESLoadConfig): Try[Option[DataFrame]] = {
    new ESLoadJob(config, storageHandler, schemaHandler).run()
  }

  def bqload(
    config: BigQueryLoadConfig,
    maybeSchema: Option[BQSchema] = None
  ): Try[Unit] = {
    new BigQuerySparkJob(config, maybeSchema).run().map(_ => ())
  }

  def jdbcload(config: JdbcLoadConfig): Try[Option[DataFrame]] = {
    val loadJob = new JdbcLoadJob(config)
    loadJob.run()
  }

  def atlas(config: AtlasConfig): Unit = {
    new AtlasJob(config, storageHandler).run()
  }

  /**
    * Runs the metrics job
    *
    * @param cliConfig : Client's configuration for metrics computing
    */
  def metric(cliConfig: MetricsConfig): Unit = {
    //Lookup for the domain given as prompt arguments, if is found then find the given schema in this domain
    val cmdArgs = for {
      domain <- schemaHandler.getDomain(cliConfig.domain)
      schema <- domain.schemas.find(_.name == cliConfig.schema)
    } yield (domain, schema)

    cmdArgs match {
      case Some((domain: Domain, schema: Schema)) =>
        val stage: Stage = cliConfig.stage.getOrElse(Stage.UNIT)
        new MetricsJob(
          domain,
          schema,
          stage,
          storageHandler,
          schemaHandler
        ).run()
      case None => logger.error("The domain or schema you specified doesn't exist! ")
    }
  }

  /**
    * Set nullable property of column.
    * @param df source DataFrame
    * @param nullable is the flag to set, such that the column is  either nullable or not
    */
  def setNullableStateOfColumn(df: DataFrame, nullable: Boolean): DataFrame = {

    // get schema
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) => StructField(c, t, nullable = nullable, m)
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }
}
