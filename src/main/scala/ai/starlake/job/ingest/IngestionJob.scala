package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.exceptions.DisallowRejectRecordException
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.loaders.{
  BigQueryNativeLoader,
  DuckDbNativeLoader,
  NativeLoader,
  SnowflakeNativeLoader
}
import ai.starlake.job.metrics.*
import ai.starlake.job.sink.bigquery.*
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.utils.*
import ai.starlake.utils.Formatter.*
import com.google.cloud.bigquery.TableId
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}

trait IngestionJob extends JobBase {
  val accessToken: Option[String]
  val test: Boolean
  val scheduledDate: Option[String]

  def domain: DomainInfo

  def schema: SchemaInfo

  def storageHandler: StorageHandler

  def schemaHandler: SchemaHandler

  def types: List[Type]

  def path: List[Path]

  def options: Map[String, String]

  def name: String =
    s"""${domain.name}-${schema.name}-${path.headOption.map(_.getName).mkString(",")}"""

  lazy val strategy: WriteStrategy = {
    val s = mergedMetadata.getStrategyOptions()
    val startTs = s.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = s.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
    s.copy(startTs = Some(startTs), endTs = Some(endTs))
  }

  def targetTableName: String = domain.finalName + "." + schema.finalName

  val now: Timestamp = java.sql.Timestamp.from(Instant.now)

  /** Merged metadata
    */
  lazy val mergedMetadata: Metadata = schema.mergedMetadata(domain.metadata)
  lazy val loader: String = mergedMetadata.loader.getOrElse(settings.appConfig.loader)

  def applyJdbcAcl(connection: Settings.ConnectionInfo, forceApply: Boolean = false): Try[Unit] = {
    val fullTableName = schemaHandler.getFullTableName(domain, schema)
    val sqls =
      schema.acl.flatMap { ace =>
        ace.asSql(fullTableName, connection.getJdbcEngineName())
      }
    AccessControlEntry.applyJdbcAcl(
      connection,
      sqls,
      forceApply
    )
  }

  private def bqNativeJob(tableId: TableId, sql: String)(implicit settings: Settings) = {
    val bqConfig = BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
      outputDatabase = schemaHandler.getDatabase(domain),
      outputTableId = Some(tableId),
      accessToken = accessToken
    )
    new BigQueryNativeJob(bqConfig, sql)
  }

  def runPrePostSql(engine: Engine, sqls: List[String]): Unit = {
    engine match {

      case Engine.BQ =>
        val connection = settings.appConfig.connections(mergedMetadata.getSinkConnectionRef())
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          schemaHandler.getDatabase(domain),
          domain.finalName,
          schema.finalName,
          connection.options.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
        )

        sqls.foreach { sql =>
          val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
          bqNativeJob(tableId, compiledSql).runBigQueryJob()
        }

      case Engine.JDBC =>
        val connection = settings.appConfig.connections(mergedMetadata.getSinkConnectionRef())
        JdbcDbUtils.withJDBCConnection(
          this.schemaHandler.dataBranch(),
          connection.withAccessToken(accessToken).options
        ) { conn =>
          sqls.foreach { sql =>
            val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
            JdbcDbUtils.executeUpdate(compiledSql, conn)
          }
        }

      case _ =>
        // Default: execute via JDBC (DuckDB)
        val connection = mergedMetadata.getSinkConnection()
        JdbcDbUtils.withJDBCConnection(
          this.schemaHandler.dataBranch(),
          connection.withAccessToken(accessToken).options
        ) { conn =>
          sqls.foreach { sql =>
            val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
            JdbcDbUtils.executeUpdate(compiledSql, conn)
          }
        }
    }
  }

  private def selectLoader(): String = {
    val sinkConn = mergedMetadata.getSinkConnection()
    val dbName = sinkConn.targetDatawareHouse()
    val nativeCandidate: Boolean = isNativeCandidate(dbName)
    logger.info(s"Native candidate: $nativeCandidate")

    val loader =
      if (nativeCandidate) {
        val loaders = Set("bigquery", "duckdb", "snowflake")
        if (loaders.contains(dbName))
          dbName
        else
          "duckdb"
      } else {
        "duckdb"
      }
    logger.info(s"Using $loader as ingestion engine")
    loader
  }

  private def isNativeCandidate(dbName: String): Boolean = {
    val nativeValidator =
      mergedMetadata.loader
        .orElse(mergedMetadata.getSinkConnection().loader)
        .getOrElse(settings.appConfig.loader)
        .toLowerCase()
        .equals("native")
    if (!nativeValidator) {
      false
    } else {
      dbName match {
        case "bigquery" =>
          val csvOrJsonLines =
            !mergedMetadata.resolveArray() && Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
              .contains(
                mergedMetadata.resolveFormat()
              )
          // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
          csvOrJsonLines
        case "duckdb" =>
          Set(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.PARQUET, Format.POSITION)
            .contains(
              mergedMetadata.resolveFormat()
            )
        case "snowflake" =>
          Set(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.XML, Format.PARQUET)
            .contains(
              mergedMetadata.resolveFormat()
            )
        case "redshift" =>
          Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
            .contains(
              mergedMetadata.resolveFormat()
            )
        case _ => false
      }
    }
  }

  def logLoadFailureInAudit(start: Timestamp, exception: Throwable): Failure[Nothing] = {
    exception.printStackTrace()
    val end = Timestamp.from(Instant.now())
    val err = Utils.exceptionAsString(exception)
    val logs = if (settings.appConfig.audit.detailedLoadAudit) {
      // create duplicated log entry for each entry path because we currently don't know which job fails at this step.
      // need more rework to target path that fails.
      // splitting by path allows to reduce size of one log entry or query which is what we are aiming at with
      // detailed load audit
      path.map { p =>
        AuditLog(
          applicationId(),
          Some(p.toString),
          domain.name,
          schema.name,
          success = false,
          0,
          0,
          0,
          start,
          end.getTime - start.getTime,
          err,
          Step.LOAD.toString,
          schemaHandler.getDatabase(domain),
          settings.appConfig.tenant,
          test = false,
          scheduledDate
        )
      }
    } else {
      List(
        AuditLog(
          applicationId(),
          Some(path.map(_.toString).mkString(",")),
          domain.name,
          schema.name,
          success = false,
          0,
          0,
          0,
          start,
          end.getTime - start.getTime,
          err,
          Step.LOAD.toString,
          schemaHandler.getDatabase(domain),
          settings.appConfig.tenant,
          test = false,
          scheduledDate
        )
      )
    }
    AuditLog.sink(logs, accessToken)(settings, storageHandler, schemaHandler)
    logger.error(err)
    Failure(exception)
  }

  def logLoadInAudit(
    start: Timestamp,
    ingestionCounters: List[IngestionCounters]
  ): Try[List[AuditLog]] = {
    val logs = ingestionCounters.map { counter =>
      val inputFiles = counter.paths.mkString(",")
      logger.info(
        s"ingestion-summary -> files: [$inputFiles], domain: ${domain.name}, schema: ${schema.name}, input: ${counter.inputCount}, accepted: ${counter.acceptedCount}, rejected:${counter.rejectedCount}"
      )
      val end = Timestamp.from(Instant.now())
      val success = !settings.appConfig.rejectAllOnError || counter.rejectedCount == 0
      AuditLog(
        applicationId(),
        Some(inputFiles),
        domain.name,
        schema.name,
        success = success,
        counter.inputCount,
        counter.acceptedCount,
        counter.rejectedCount,
        start,
        end.getTime - start.getTime,
        if (success) "success" else s"${counter.rejectedCount} invalid records",
        Step.LOAD.toString,
        schemaHandler.getDatabase(domain),
        settings.appConfig.tenant,
        test = false,
        scheduledDate
      )
    }
    AuditLog.sink(logs, accessToken)(settings, storageHandler, schemaHandler).map(_ => logs)
  }

  @throws[Exception]
  private def checkDomainValidity(): Unit = {
    domain.checkValidity(schemaHandler) match {
      case Left(errors) =>
        val errs = errors.map(_.toString()).reduce { (errs, err) =>
          errs + "\n" + err
        }
        throw new Exception(s"-- $name --\n" + errs)
      case Right(_) =>
    }
  }
  def buildListOfSQLStatementsAsMap(orchestrator: String): Map[String, Object] = {
    // Run selected ingestion engine
    val result =
      orchestrator match {
        case "bigquery" =>
          ???
        case "duckdb" =>
          ???
        case "snowflake" =>
          val statementsMap = new NativeLoader(this, None).buildSQLStatements()
          statementsMap
        case other =>
          throw new Exception(s"Unsupported engine $other")
      }
    result
  }

  def run(): Try[JobResult] = {
    // Make sure domain is valid
    // checkDomainValidity()
    val engineName = this.mergedMetadata.getSinkConnection().getJdbcEngineName()
    val engine = settings.appConfig.jdbcEngines
      .getOrElse(engineName.toString, "spark")

    // Run selected ingestion engine
    val jobResult = selectLoader() match {
      case "bigquery" =>
        val ingestionCounters = new BigQueryNativeLoader(this, accessToken).run()
        ingestionCounters
      case "snowflake" =>
        val ingestionCounters = new SnowflakeNativeLoader(this).run()
        ingestionCounters
      case "duckdb" =>
        val ingestionCounters = new DuckDbNativeLoader(this).run()
        ingestionCounters
      case other =>
        logger.warn(s"Unsupported engine $other, falling back to duckdb")
        val ingestionCounters = new DuckDbNativeLoader(this).run()
        ingestionCounters
    }
    jobResult
      .recoverWith { case exception =>
        // on failure log failures
        logLoadFailureInAudit(now, exception)
      }
      .map { (counterResults: List[IngestionCounters]) =>
        val validCounterResults = counterResults.filter(!_.ignore)
        logLoadInAudit(now, validCounterResults) match {
          case Failure(exception) => throw exception
          case Success(auditLogs) =>
            if (auditLogs.exists(!_.success)) {
              throw new DisallowRejectRecordException()
            }
        }
        // run expectations
        val expectationsResult = runExpectations()
        expectationsResult match {
          case Failure(exception) if settings.appConfig.expectations.failOnError =>
            throw exception
          case _ =>
        }
        val globalCounters: Option[IngestionCounters] =
          validCounterResults.foldLeft[Option[IngestionCounters]](None) {
            case (
                  cumulatedCounters,
                  counters @ IngestionCounters(
                    inputCount,
                    acceptedCount,
                    rejectedCount,
                    paths,
                    jobid
                  )
                ) =>
              cumulatedCounters
                .map { cc =>
                  IngestionCounters(
                    inputCount = cc.inputCount + inputCount,
                    acceptedCount = cc.acceptedCount + acceptedCount,
                    rejectedCount = cc.rejectedCount + rejectedCount,
                    paths = cc.paths ++ paths,
                    jobid = this.applicationId() + "," + jobid
                  )

                }
                .orElse(Some(counters))
          }
        IngestionJobResult(
          globalCounters.orElse(
            Some(
              IngestionCounters(
                inputCount = -1,
                acceptedCount = -1,
                rejectedCount = -1,
                paths = path.map(_.toString),
                jobid = applicationId()
              )
            )
          )
        )
      }
  }

  private def runExpectations(): Try[JobResult] = {
    mergedMetadata.getSink() match {
      case _: BigQuerySink =>
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          settings.appConfig.audit.getDatabase(),
          settings.appConfig.audit.getDomain(),
          "expectations",
          mergedMetadata
            .getSinkConnection()
            .options
            .get("projectId")
            .orElse(settings.appConfig.getDefaultDatabase())
        )
        runBigQueryExpectations(bqNativeJob(tableId, ""))
      case _ =>
        val options = mergedMetadata.getSinkConnection().withAccessToken(accessToken).options
        runJdbcExpectations(options)
    }
  }

  private def runExpectationsWithHandler(handler: ExpectationAssertionHandler): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        Option(applicationId()),
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        handler,
        false
      ).run()
    } else {
      Success(JdbcJobResult(Nil))
    }
  }

  private def runJdbcExpectations(jdbcOptions: Map[String, String]): Try[JobResult] =
    runExpectationsWithHandler(new JdbcExpectationAssertionHandler(jdbcOptions))

  def runBigQueryExpectations(job: BigQueryNativeJob): Try[JobResult] =
    runExpectationsWithHandler(new BigQueryExpectationAssertionHandler(job))
}
