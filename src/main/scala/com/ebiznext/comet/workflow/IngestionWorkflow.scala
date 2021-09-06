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
import com.ebiznext.comet.job.index.connectionload.{ConnectionLoadConfig, ConnectionLoadJob}
import com.ebiznext.comet.job.index.esload.{ESLoadConfig, ESLoadJob}
import com.ebiznext.comet.job.index.kafkaload.{KafkaJob, KafkaJobConfig}
import com.ebiznext.comet.job.infer.{InferSchema, InferSchemaConfig}
import com.ebiznext.comet.job.ingest._
import com.ebiznext.comet.job.load.LoadStrategy
import com.ebiznext.comet.job.metrics.{MetricsConfig, MetricsJob}
import com.ebiznext.comet.job.transform.AutoTaskJob
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Format._
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils._
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{Schema => BQSchema}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

import java.nio.file.{FileSystems, ProviderNotFoundException}
import java.util.Collections
import scala.util.{Failure, Success, Try}

/** The whole worklfow works as follow :
  *   - loadLanding : Zipped files are uncompressed or raw files extracted from the local
  *     filesystem.
  * -loadPending : files recognized with filename patterns are stored in the ingesting area and
  * submitted for ingestion files with unrecognized filename patterns are stored in the unresolved
  * area
  *   - ingest : files are finally ingested and saved as parquet/orc/... files and hive tables
  *
  * @param storageHandler
  *   : Minimum set of features required for the underlying filesystem
  * @param schemaHandler
  *   : Schema interface
  * @param launchHandler
  *   : Cron Manager interface
  */
class IngestionWorkflow(
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  launchHandler: LaunchHandler
)(implicit settings: Settings)
    extends StrictLogging {
  val domains: List[Domain] = schemaHandler.domains

  /** Move the files from the landing area to the pending area. files are loaded one domain at a
    * time each domain has its own directory and is specified in the "directory" key of Domain YML
    * file compressed files are uncompressed if a corresponding ack file exist. Compressed files are
    * recognized by their extension which should be one of .tgz, .zip, .gz. raw file should also
    * have a corresponding ack file before moving the files to the pending area, the ack files are
    * deleted To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML
    * file. "ack" is the default ack extension searched for but you may specify a different one in
    * the domain YML file.
    */
  def loadLanding(): Unit = {
    logger.info("LoadLanding")
    domains.foreach { domain =>
      val storageHandler = settings.storageHandler
      val inputDir = new Path(domain.directory)
      if (storageHandler.exists(inputDir)) {
        logger.info(s"Scanning $inputDir")
        storageHandler.list(inputDir, domain.getAck(), recursive = false).foreach { path =>
          val (filesToLoad, tmpDir) = {
            def asBetterFile(path: Path): File = {
              Try {
                File(path.toUri)
              } match {
                case Success(file) => file
                case Failure(_: ProviderNotFoundException) =>
                  // if a FileSystem is available in the classpath, it will be installed
                  FileSystems
                    .newFileSystem(path.toUri, Collections.emptyMap(), getClass.getClassLoader)
                  File(path.toUri) // retry
                case Failure(exception) => throw exception
              }
            }

            val pathWithoutLastExt = new Path(
              path.getParent, // works as long this is not recursive
              if (domain.getAck().isEmpty) asBetterFile(path).nameWithoutExtension(false)
              else path.getName.stripSuffix(domain.getAck())
            )

            val zipExtensions = List(".gz", ".tgz", ".zip")
            val (existingRawFile, existingArchiveFile) = {
              val findPathWithExt = if (domain.getAck().isEmpty) {
                // the file is always the one being iterated over, we just need to check the extension
                (extensions: List[String]) => extensions.find(path.getName.endsWith).map(_ => path)
              } else {
                // for each extension, look if there exists a file near the .ack one
                (extensions: List[String]) =>
                  extensions.map(pathWithoutLastExt.suffix).find(storageHandler.exists)
              }
              (findPathWithExt(domain.getExtensions()), findPathWithExt(zipExtensions))
            }

            if (domain.getAck().nonEmpty)
              storageHandler.delete(path)

            (existingArchiveFile, existingRawFile, storageHandler.fs.getScheme) match {
              case (Some(zipPath), _, "file") =>
                logger.info(s"Found compressed file $zipPath")
                storageHandler.mkdirs(pathWithoutLastExt)

                // File is a proxy to java.nio.Path / working with Uri
                val tmpDir = asBetterFile(pathWithoutLastExt)
                val zipFile = asBetterFile(zipPath)
                zipFile.extension() match {
                  case Some(".tgz") => Unpacker.unpack(zipFile, tmpDir)
                  case Some(".gz")  => zipFile.unGzipTo(tmpDir / pathWithoutLastExt.getName)
                  case Some(".zip") => zipFile.unzipTo(tmpDir)
                  case _            => logger.error(s"Unsupported archive type for $zipFile")
                }
                storageHandler.delete(zipPath)

                (
                  storageHandler
                    .list(pathWithoutLastExt, recursive = false)
                    .filter(path => domain.getExtensions().exists(path.getName.endsWith)),
                  Some(pathWithoutLastExt)
                )
              case (_, Some(filePath), _) =>
                logger.info(s"Found raw file $filePath")
                (List(filePath), None)
              case (_, _, _) =>
                logger.info(s"Ignoring file for path $path")
                (List.empty, None)
            }
          }

          val destFolder = DatasetArea.pending(domain.name)
          filesToLoad.foreach { file =>
            logger.info(s"Importing $file")
            val destFile = new Path(destFolder, file.getName)
            storageHandler.moveFromLocal(file, destFile)
          }

          tmpDir.foreach(storageHandler.delete)
        }
      } else {
        logger.error(s"Input path : $inputDir not found, ${domain.name} Domain is ignored")
      }
    }
  }

  /** Split files into resolved and unresolved datasets. A file is unresolved if a corresponding
    * schema is not found. Schema matching is based on the dataset filename pattern
    *
    * @param config
    *   : includes Load pending dataset of these domain only excludes : Do not load datasets of
    *   these domains if both lists are empty, all domains are included
    */
  def loadPending(config: WatchConfig = WatchConfig()): Boolean = {
    val includedDomains = (config.includes, config.excludes) match {
      case (Nil, Nil) =>
        domains
      case (_, Nil) =>
        domains.filter(domain => config.includes.contains(domain.name))
      case (Nil, _) =>
        domains.filter(domain => !config.excludes.contains(domain.name))
      case (_, _) => throw new Exception("Should never happen ")
    }
    logger.info(s"Domains that will be watched: ${includedDomains.map(_.name).mkString(",")}")

    val result: List[Boolean] = includedDomains.flatMap { domain =>
      logger.info(s"Watch Domain: ${domain.name}")
      val (resolved, unresolved) = pending(domain.name, config.schemas.toList)
      unresolved.foreach { case (_, path) =>
        val targetPath =
          new Path(DatasetArea.unresolved(domain.name), path.getName)
        logger.info(s"Unresolved file : ${path.getName}")
        storageHandler.move(path, targetPath)
      }

      val filteredResolved = if (settings.comet.privacyOnly) {
        val (withPrivacy, noPrivacy) =
          resolved.partition { case (schema, _) =>
            schema.exists(_.attributes.map(_.getPrivacy()).exists(!PrivacyLevel.None.equals(_)))
          }
        // files for schemas without any privacy attributes are moved directly to accepted area
        noPrivacy.foreach {
          case (Some(schema), path) =>
            storageHandler.move(
              path,
              new Path(new Path(DatasetArea.accepted(domain.name), schema.name), path.getName)
            )
          case (None, _) => throw new Exception("Should never happen")
        }
        withPrivacy
      } else {
        resolved
      }

      // We group files with the same schema to ingest them together in a single step.
      val groupedResolved: Map[Schema, Iterable[Path]] = filteredResolved.map {
        case (Some(schema), path) => (schema, path)
        case (None, _)            => throw new Exception("Should never happen")
      } groupBy { case (schema, _) => schema } mapValues (it => it.map { case (_, path) => path })

      groupedResolved.map { case (schema, pendingPaths) =>
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
        val resTry = Try {
          if (settings.comet.grouped) {
            val res =
              launchHandler.ingest(this, domain, schema, ingestingPaths.toList, config.options)
            res.isSuccess
          } else {
            // We ingest all the files but return false if one them fails.
            val res = ingestingPaths.map { path =>
              launchHandler.ingest(this, domain, schema, path, config.options)
            }
            res.forall(_.isSuccess)
          }
        }
        resTry match {
          case Failure(e) =>
            e.printStackTrace()
            false
          case Success(r) => r
        }
      }
    }
    result.forall(_ == true)
  }

  /** @param domainName
    *   : Domaine name
    * @return
    *   resolved && unresolved schemas / path
    */
  private def pending(
    domainName: String,
    schemasName: List[String]
  ): (Iterable[(Option[Schema], Path)], Iterable[(Option[Schema], Path)]) = {
    val pendingArea = DatasetArea.pending(domainName)
    logger.info(s"List files in $pendingArea")
    val files = Utils
      .loadInstance[LoadStrategy](settings.comet.loadStrategyClass)
      .list(settings.storageHandler.fs, pendingArea, recursive = false)
    if (files.nonEmpty)
      logger.info(s"Found ${files.mkString(",")}")
    else
      logger.info(s"No Files Found.")
    val domain = schemaHandler.getDomain(domainName)

    val filteredFiles = (dom: Domain) =>
      if (schemasName.nonEmpty) {
        logger.info(
          s"We will only watch files that match the schemas name:" +
          s" $schemasName for the Domain: ${dom.name}"
        )
        files.filter(f => predicate(dom, schemasName, f))
      } else {
        logger.info(
          s"We will watch all the files for the Domain:" +
          s" ${dom.name}"
        )
        files
      }

    val schemas = for {
      dom <- domain.toList
      (schema, path) <- filteredFiles(dom).map { file =>
        (dom.findSchema(file.getName), file)
      }
    } yield {
      logger.info(
        s"Found Schema ${schema.map(_.name).getOrElse("None")} for file $path"
      )
      (schema, path)
    }
    schemas.partition { case (schema, _) => schema.isDefined }
  }

  private def predicate(domain: Domain, schemasName: List[String], file: Path): Boolean = {
    schemasName.exists { schemaName =>
      val schema = domain.schemas.find(_.name.equals(schemaName))
      schema.exists(_.pattern.matcher(file.getName).matches())
    }
  }

  /** Ingest the file (called by the cron manager at ingestion time for a specific dataset
    */
  def load(config: LoadConfig): Boolean = {
    val lockPath =
      new Path(settings.comet.lock.path, s"${config.domain}_${config.schema}.lock")
    val locker = new FileLock(lockPath, storageHandler)
    val waitTimeMillis = settings.comet.lock.timeout

    locker.doExclusively(waitTimeMillis) {
      val domainName = config.domain
      val schemaName = config.schema
      val ingestingPaths = config.paths
      val result = for {
        domain <- domains.find(_.name == domainName)
        schema <- domain.schemas.find(_.name == schemaName)
      } yield ingest(domain, schema, ingestingPaths, config.options)
      result match {
        case None | Some(Success(_)) => true
        case Some(Failure(exception)) =>
          Utils.logException(logger, exception)
          false
      }
    }
  }

  def ingest(
    domain: Domain,
    schema: Schema,
    ingestingPath: List[Path],
    options: Map[String, String]
  ): Try[JobResult] = {
    logger.info(
      s"Start Ingestion on domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath"
    )
    val metadata = domain.metadata
      .getOrElse(Metadata())
      .`import`(schema.metadata.getOrElse(Metadata()))
    logger.info(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath with metadata $metadata"
    )
    val ingestionResult = Try {
      metadata.getFormat() match {
        case DSV =>
          new DsvIngestionJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options
          ).run()
        case SIMPLE_JSON =>
          new SimpleJsonIngestionJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options
          ).run()
        case JSON =>
          new JsonIngestionJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options
          ).run()
        case XML =>
          new XmlIngestionJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options
          ).run()
        case TEXT_XML =>
          new XmlSimplePrivacyJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options
          ).run()
        case POSITION =>
          new PositionIngestionJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options
          ).run()
        case KAFKA =>
          new KafkaIngestionJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options,
            Mode.FILE
          ).run()
        case KAFKASTREAM =>
          new KafkaIngestionJob(
            domain,
            schema,
            schemaHandler.types,
            ingestingPath,
            storageHandler,
            schemaHandler,
            options,
            Mode.STREAM
          ).run()
        case _ =>
          throw new Exception("Should never happen")
      }
    }
    ingestionResult match {
      case Success(Success(jobResult)) =>
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
        Success(jobResult)
      case Success(Failure(exception)) =>
        Utils.logException(logger, exception)
        Failure(exception)
      case Failure(exception) =>
        Utils.logException(logger, exception)
        Failure(exception)
    }
  }

  def infer(config: InferSchemaConfig): Try[Unit] = {
    val result = new InferSchema(
      config.domainName,
      config.schemaName,
      config.inputPath,
      config.outputPath,
      config.header
    ).run()
    Utils.logFailure(result, logger)
  }

  def buildTasks(jobName: String, jobOptions: Map[String, String]): Seq[AutoTaskJob] = {
    val job = schemaHandler.jobs(jobName)
    logger.info(job.toString)
    val includes = schemaHandler.views(job.name)
    job.tasks.map { task =>
      AutoTaskJob(
        job.name,
        job.getArea(),
        job.format,
        job.coalesce.getOrElse(false),
        job.udf,
        Views(job.views.getOrElse(Map.empty) ++ includes.views),
        job.getEngine(),
        task,
        schemaHandler.activeEnv ++ jobOptions
      )(settings, storageHandler, schemaHandler)
    }
  }

  /** Successively run each task of a job
    *
    * @param config
    *   : job name as defined in the YML file and sql parameters to pass to SQL statements.
    */
  def autoJob(config: TransformConfig): Boolean = {
    val job = schemaHandler.jobs(config.name)
    logger.info(job.toString)
    val includes = schemaHandler.views(job.name)
    val result: Seq[Boolean] = buildTasks(config.name, config.options).map { action =>
      val engine = action.engine
      logger.info(s"running with $engine engine")
      engine match {
        case Engine.BQ =>
          val result = config.views match {
            case Nil => // User asked to executed the whole job
              val result = action.runBQ()
              val sink = action.task.sink
              logger.info(s"BQ Job succeeded. sinking data to $sink")
              sink match {
                case Some(sink) if sink.`type` == SinkType.BQ =>
                  logger.info("Sinking to BQ done")
                case _ =>
                  // TODO Sinking not supported
                  logger.error(s"Sinking from BQ to $sink not yet supported.")
              }
              result
            case queryNames => // User asked to executed one or more views only (for debugging purpose probably)
              val queries =
                if (queryNames.contains("_") || queryNames.contains("*"))
                  job.views.map(_.keys).getOrElse(Nil)
                else
                  queryNames
              val result = queries.map(queryName =>
                action.runView(queryName, config.viewsDir, config.viewsCount)
              )
              result.filter(_.isFailure) match {
                case Nil =>
                  result.headOption.getOrElse(
                    Failure(
                      new Exception(
                        s"No view with the provided view names '$queryNames' has been found"
                      )
                    )
                  )
                case errors =>
                  // We return all failures
                  Failure(errors.collect { case Failure(e) => e }.reduce(_.initCause(_)))
              }
          }
          Utils.logFailure(result, logger)
          result.isSuccess
        case Engine.SPARK =>
          action.runSpark() match {
            case Success(SparkJobResult(maybeDataFrame)) =>
              val sinkOption = action.task.sink
              logger.info(s"Spark Job succeeded. sinking data to $sinkOption")
              sinkOption match {
                case Some(sink) => {
                  sink.`type` match {
                    case SinkType.ES if settings.comet.elasticsearch.active =>
                      esload(action)
                    case SinkType.BQ =>
                      val bqSink = sink.asInstanceOf[BigQuerySink]
                      val source = maybeDataFrame
                        .map(df => Right(setNullableStateOfColumn(df, nullable = true)))
                        .getOrElse(Left(action.task.getTargetPath(job.getArea()).toString))
                      val (createDisposition, writeDisposition) = {
                        Utils.getDBDisposition(action.task.write, hasMergeKeyDefined = false)
                      }
                      val config =
                        BigQueryLoadConfig(
                          source = source,
                          outputTable = action.task.dataset,
                          outputDataset = action.task.domain,
                          sourceFormat = "parquet",
                          createDisposition = createDisposition,
                          writeDisposition = writeDisposition,
                          location = bqSink.location,
                          outputPartition = bqSink.timestamp,
                          outputClustering = bqSink.clustering.getOrElse(Nil),
                          days = bqSink.days,
                          requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
                          rls = action.task.rls,
                          options = bqSink.getOptions
                        )
                      val result = new BigQuerySparkJob(config, None).run()
                      result.isSuccess

                    case SinkType.JDBC =>
                      val jdbcSink = sink.asInstanceOf[JdbcSink]
                      val partitions = jdbcSink.partitions.getOrElse(1)
                      val batchSize = jdbcSink.batchsize.getOrElse(1000)
                      val jdbcName = jdbcSink.connection
                      val source = maybeDataFrame
                        .map(df => Right(df))
                        .getOrElse(Left(action.task.getTargetPath(job.getArea()).toString))
                      val (createDisposition, writeDisposition) = {
                        Utils.getDBDisposition(action.task.write, hasMergeKeyDefined = false)
                      }
                      val jdbcConfig = ConnectionLoadConfig.fromComet(
                        jdbcName,
                        settings.comet,
                        source,
                        outputTable = action.task.dataset,
                        createDisposition = CreateDisposition.valueOf(createDisposition),
                        writeDisposition = WriteDisposition.valueOf(writeDisposition),
                        partitions = partitions,
                        batchSize = batchSize,
                        createTableIfAbsent = false,
                        options = jdbcSink.getOptions
                      )

                      val res = new ConnectionLoadJob(jdbcConfig).run()
                      res match {
                        case Success(_) => true
                        case Failure(e) => logger.error("JDBCLoad Failed", e); false
                      }
                    case _ =>
                      logger.warn("No supported Sink is activated for this job")
                      true
                  }
                }
                case _ =>
                  logger.warn("Sink is not activated for this job")
                  true
              }
            case Failure(exception) =>
              exception.printStackTrace()
              false
          }
        case _ =>
          logger.error("Should never happen")
          false
      }
    }
    result.forall(_ == true)
  }

  def esload(action: AutoTaskJob): Boolean = {
    val targetArea = action.task.area.getOrElse(action.defaultArea)
    val targetPath =
      new Path(DatasetArea.path(action.task.domain, targetArea.value), action.task.dataset)
    val sink: EsSink = action.task.sink
      .map(_.asInstanceOf[EsSink])
      .getOrElse(
        throw new Exception("Sink of type ES must be specified when loading data to ES !!!")
      )
    launchHandler.esLoad(
      this,
      ESLoadConfig(
        timestamp = sink.timestamp,
        id = sink.id,
        format = "parquet",
        domain = action.task.domain,
        schema = action.task.dataset,
        dataset = Some(Left(targetPath)),
        options = sink.getOptions
      )
    )
  }

  /** Set nullable property of column.
    * @param df
    *   source DataFrame
    * @param nullable
    *   is the flag to set, such that the column is either nullable or not
    */
  def setNullableStateOfColumn(df: DataFrame, nullable: Boolean): DataFrame = {

    // get schema
    val schema = df.schema
    val newSchema = StructType(schema.map { case StructField(c, t, _, m) =>
      StructField(c, t, nullable = nullable, m)
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def esLoad(config: ESLoadConfig): Try[JobResult] = {
    val res = new ESLoadJob(config, storageHandler, schemaHandler).run()
    Utils.logFailure(res, logger)
  }

  def bqload(
    config: BigQueryLoadConfig,
    maybeSchema: Option[BQSchema] = None
  ): Try[JobResult] = {
    val res = new BigQuerySparkJob(config, maybeSchema).run()
    Utils.logFailure(res, logger)
  }

  def kafkaload(config: KafkaJobConfig): Try[JobResult] = {
    val res = new KafkaJob(config).run()
    Utils.logFailure(res, logger)
  }

  def jdbcload(config: ConnectionLoadConfig): Try[JobResult] = {
    val loadJob = new ConnectionLoadJob(config)
    val res = loadJob.run()
    Utils.logFailure(res, logger)
  }

  def atlas(config: AtlasConfig): Boolean = {
    new AtlasJob(config, storageHandler).run()
  }

  /** Runs the metrics job
    *
    * @param cliConfig
    *   : Client's configuration for metrics computing
    */
  def metric(cliConfig: MetricsConfig): Try[JobResult] = {
    //Lookup for the domain given as prompt arguments, if is found then find the given schema in this domain
    val cmdArgs = for {
      domain <- schemaHandler.getDomain(cliConfig.domain)
      schema <- domain.schemas.find(_.name == cliConfig.schema)
    } yield (domain, schema)

    cmdArgs match {
      case Some((domain: Domain, schema: Schema)) =>
        val stage: Stage = cliConfig.stage.getOrElse(Stage.UNIT)
        val result = new MetricsJob(
          domain,
          schema,
          stage,
          storageHandler,
          schemaHandler
        ).run()
        Utils.logFailure(result, logger)
      case None =>
        logger.error("The domain or schema you specified doesn't exist! ")
        Failure(new Exception("The domain or schema you specified doesn't exist! "))
    }
  }
}
