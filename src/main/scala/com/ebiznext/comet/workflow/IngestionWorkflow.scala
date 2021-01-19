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
import com.ebiznext.comet.job.index.connectionload.{ConnectionLoadConfig, ConnectionLoadJob}
import com.ebiznext.comet.job.infer.{InferSchema, InferSchemaConfig}
import com.ebiznext.comet.job.ingest._
import com.ebiznext.comet.job.metrics.{MetricsConfig, MetricsJob}
import com.ebiznext.comet.job.transform.AutoTaskJob
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Format.{DSV, JSON, POSITION, SIMPLE_JSON, XML}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.{JobResult, SparkJobResult, Unpacker, Utils}
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{Schema => BQSchema}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.{Failure, Success, Try}

/** The whole worklfow works as follow :
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

  /** Move the files from the landing area to the pending area.
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
      if (storageHandler.exists(inputDir)) {
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
      } else {
        logger.error(s"Input path : $inputDir not found, ${domain.name} Domain is ignored")
      }
    }
  }

  /** Split files into resolved and unresolved datasets. A file is unresolved
    * if a corresponding schema is not found.
    * Schema matching is based on the dataset filename pattern
    *
    * @param config : includes Load pending dataset of these domain only
    *                 excludes : Do not load datasets of these domains
    *                 if both lists are empty, all domains are included
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

    val result = includedDomains.flatMap { domain =>
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
          resolved.partition(
            _._1.exists(_.attributes.map(_.privacy).exists(!PrivacyLevel.None.equals(_)))
          )
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
      } groupBy (_._1) mapValues (it => it.map(_._2))

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
        try {
          if (settings.comet.grouped)
            launchHandler.ingest(this, domain, schema, ingestingPaths.toList, config.options)
          else {
            // We ingest all the files but return false if one them fails.
            ingestingPaths
              .map(launchHandler.ingest(this, domain, schema, _, config.options))
              .forall(_ == true)
          }
        } catch {
          case t: Throwable =>
            t.printStackTrace()
            false
        }
      }
    }
    result.forall(_ == true)
  }

  /** @param domainName : Domaine name
    * @return resolved && unresolved schemas / path
    */
  private def pending(
    domainName: String,
    schemasName: List[String]
  ): (Iterable[(Option[Schema], Path)], Iterable[(Option[Schema], Path)]) = {
    val pendingArea = DatasetArea.pending(domainName)
    logger.info(s"List files in $pendingArea")
    val files = storageHandler.list(pendingArea)
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
      schema <- filteredFiles(dom).map { file =>
        (dom.findSchema(file.getName), file)
      }
    } yield {
      logger.info(
        s"Found Schema ${schema._1.map(_.name).getOrElse("None")} for file ${schema._2}"
      )
      schema
    }
    schemas.partition(_._1.isDefined)
  }

  private[this] def predicate(domain: Domain, schemasName: List[String], file: Path): Boolean = {
    schemasName.exists { schemaName =>
      val schema = domain.schemas.find(_.name.equals(schemaName))
      schema.exists(_.pattern.matcher(file.getName).matches())
    }
  }

  /** Ingest the file (called by the cron manager at ingestion time for a specific dataset
    */
  def ingest(config: LoadConfig): Boolean = {
    val domainName = config.domain
    val schemaName = config.schema
    val ingestingPaths = config.paths
    val result = for {
      domain <- domains.find(_.name == domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield ingesting(domain, schema, ingestingPaths, config.options)
    result.getOrElse(true)
  }

  private def ingesting(
    domain: Domain,
    schema: Schema,
    ingestingPath: List[Path],
    options: Map[String, String]
  ): Boolean = {
    logger.info(
      s"Start Ingestion on domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath"
    )
    val metadata = domain.metadata
      .getOrElse(Metadata())
      .`import`(schema.metadata.getOrElse(Metadata()))
    logger.info(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath with metadata $metadata"
    )
    val ingestionResult: Try[JobResult] = Try(metadata.getFormat() match {
      case DSV =>
        new DsvIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler,
          options
        ).run().get
      case SIMPLE_JSON =>
        new SimpleJsonIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler,
          options
        ).run().get
      case JSON =>
        new JsonIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler,
          options
        ).run().get
      case XML =>
        new XmlIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler,
          options
        ).run().get
      case POSITION =>
        new PositionIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler,
          schemaHandler,
          options
        ).run().get
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
    ingestionResult.isSuccess
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

  /** Successively run each task of a job
    *
    * @param config : job name as defined in the YML file and sql parameters to pass to SQL statements.
    */
  def autoJob(config: TransformConfig): Boolean = {
    val job = schemaHandler.jobs(config.name)
    val activeEnv = schemaHandler.activeEnv
    logger.info(job.toString)
    val includes = schemaHandler.views(job.name)
    val result = job.tasks.map { task =>
      val action = new AutoTaskJob(
        job.name,
        job.area,
        job.format,
        job.coalesce.getOrElse(false),
        job.udf,
        Views(job.views.getOrElse(Map.empty) ++ includes.views),
        job.getEngine(),
        task,
        storageHandler,
        config.options,
        schemaHandler
      )
      val engine = job.getEngine()
      logger.info(s"running with $engine engine")
      engine match {
        case Engine.BQ =>
          val result = config.views match {
            case Nil =>
              val result = action.runBQ()
              val sink = task.sink
              logger.info(s"BQ Job succeeded. sinking data to $sink")
              sink match {
                case Some(sink) if sink.`type` == SinkType.BQ =>
                  logger.info("Sinking to BQ done")
                case _ =>
                  // TODO Sinking not supported
                  logger.error(s"Sinking from BQ to $sink not yet supported.")
              }
              result
            case queryNames =>
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
                  Failure(errors.map(_.failed).map(_.get).reduce(_.initCause(_)))
              }
          }
          Utils.logFailure(result, logger)
          result.isSuccess
        case Engine.SPARK =>
          action.runSpark() match {
            case Success(SparkJobResult(maybeDataFrame)) =>
              val sink = task.sink
              logger.info(s"Spark Job succeeded. sinking data to $sink")
              sink match {
                case Some(sink)
                    if settings.comet.elasticsearch.active && sink.`type` == SinkType.ES =>
                  esload(job, task)
                case Some(sink) if sink.`type` == SinkType.BQ =>
                  val bqSink = sink.asInstanceOf[BigQuerySink]
                  val source = maybeDataFrame
                    .map(df => Right(setNullableStateOfColumn(df, nullable = true)))
                    .getOrElse(Left(task.getTargetPath(Some(job.getArea())).toString))
                  val (createDisposition, writeDisposition) = {
                    Utils.getDBDisposition(task.write, hasMergeKeyDefined = false)
                  }
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
                  val result = new BigQuerySparkJob(config, None).run()
                  result.isSuccess

                case Some(sink) if sink.`type` == SinkType.JDBC =>
                  val jdbcSink = sink.asInstanceOf[JdbcSink]
                  val partitions = jdbcSink.partitions.getOrElse(1)
                  val batchSize = jdbcSink.batchsize.getOrElse(1000)
                  val jdbcName = jdbcSink.connection
                  val source = maybeDataFrame
                    .map(df => Right(df))
                    .getOrElse(Left(task.getTargetPath(Some(job.getArea())).toString))
                  val (createDisposition, writeDisposition) = {
                    Utils.getDBDisposition(task.write, hasMergeKeyDefined = false)
                  }

                  val jdbcConfig = ConnectionLoadConfig.fromComet(
                    jdbcName,
                    settings.comet,
                    source,
                    outputTable = task.dataset,
                    createDisposition = CreateDisposition.valueOf(createDisposition),
                    writeDisposition = WriteDisposition.valueOf(writeDisposition),
                    partitions = partitions,
                    batchSize = batchSize,
                    createTableIfAbsent = false
                  )

                  val res = new ConnectionLoadJob(jdbcConfig).run()
                  res match {
                    case Success(_) => ;
                    case Failure(e) => logger.error("JDBCLoad Failed", e)
                  }
                case _ =>
                  // TODO Sinking not supported
                  logger.error(s"Sinking from Spark to $sink not yet supported.")
                  false
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

  def esload(job: AutoJobDesc, task: AutoTaskDesc): Boolean = {
    val targetArea = task.area.getOrElse(job.getArea())
    val targetPath = new Path(DatasetArea.path(task.domain, targetArea.value), task.dataset)
    val sink: EsSink = task.sink
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
        domain = task.domain,
        schema = task.dataset,
        dataset = Some(Left(targetPath))
      )
    )
  }

  /** Set nullable property of column.
    * @param df source DataFrame
    * @param nullable is the flag to set, such that the column is  either nullable or not
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
    * @param cliConfig : Client's configuration for metrics computing
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
