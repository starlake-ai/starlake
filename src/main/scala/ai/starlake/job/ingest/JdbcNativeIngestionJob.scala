package ai.starlake.job.ingest

import ai.starlake.config.Settings
/*
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.job.sink.jdbc.{JdbcConnectionLoadConfig, SparkJdbcWriter}
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JobResult, SparkJobResult, SparkUtils, Utils}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StructField, TimestampType}

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}
 */
/** This class is responsible for loading data from a JDBC source to a JDBC sink. Transform is done
  * by the JDBC engine
  * @param ingestionJob
  * @param settings
  */
@deprecated("Use IngestionJob instead", "1.0.0")
class JdbcNativeIngestionJob(ingestionJob: IngestionJob)(implicit val settings: Settings) {
  /*
    extends StrictLogging {
  val domain: Domain = ingestionJob.domain

  val schema: Schema = ingestionJob.schema

  val storageHandler: StorageHandler = ingestionJob.storageHandler

  val schemaHandler: SchemaHandler = ingestionJob.schemaHandler

  val path: List[Path] = ingestionJob.path

  val options: Map[String, String] = ingestionJob.options

  val strategy: StrategyOptions = schema.getStrategy(Some(ingestionJob.mergedMetadata))

  lazy val mergedMetadata: Metadata = ingestionJob.mergedMetadata

  private def runPreSql(): Unit = {
    schema.presql.foreach { sql =>
      val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
      SparkUtils.sql(ingestionJob.session, compiledSql)
    }
  }
  private def runExpectations(
    connection: java.sql.Connection
  ): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {

      new ExpectationJob(
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        new JdbcExpectationAssertionHandler(connection)
      ).run()
    } else {
      Success(SparkJobResult(None))
    }
  }

  def run(): Try[JobResult] = {
    ingestionJob.session.sparkContext.setLocalProperty(
      "spark.scheduler.pool",
      settings.appConfig.sparkScheduling.poolName
    )
    val jobResult = domain.checkValidity(schemaHandler) match {
      case Left(errors) =>
        val errs = errors.map(_.toString()).reduce { (errs, err) =>
          errs + "\n" + err
        }
        Failure(throw new Exception(s"-- Domain ${domain.name} --\n" + errs))
      case Right(_) =>
        val start = Timestamp.from(Instant.now())
        ingestionJob.runPrePostSql(mergedMetadata.getEngine(), schema.presql)
        val dataset = ingestionJob.loadDataSet(true)
        dataset match {
          case Success(dataset) =>
            val jdbcSink = mergedMetadata.getSink().asInstanceOf[JdbcSink]
            val targetTable = domain.finalName + "." + schema.finalName
            val twoSteps = requireTwoSteps(schema, jdbcSink)
            val connectionRefOptions = mergedMetadata.getSinkConnectionRefOptions()
            val isSCD2 = strategy.`type` == StrategyType.SCD2
            val firstStepTempTable =
              if (!twoSteps) {
                targetTable
              } else {
                domain.finalName + "." + SQLUtils.temporaryTableName(schema.finalName)
              }
            val result = Try {
              val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
                mergedMetadata.getWrite()
              )
              val datasetWithRenamedAttributes = ingestionJob.dfWithAttributesRenamed(dataset)
              val jdbcConnectionLoadConfig = JdbcConnectionLoadConfig(
                sourceFile = Right(datasetWithRenamedAttributes),
                outputDomainAndTableName = firstStepTempTable,
                strategy = strategy,
                format = "jdbc",
                options = connectionRefOptions
              )
              // Create table and load the data using Spark
              val jdbcJob = new SparkJdbcWriter(jdbcConnectionLoadConfig)
              val firstStepResult = jdbcJob.run()

              if (twoSteps) { // We need to apply transform, filter, merge, ...
                firstStepResult match {
                  case Success(loadFileResult) =>
                    logger.info(s"First step result: $loadFileResult")
                    JdbcDbUtils.withJDBCConnection(connectionRefOptions) { conn =>
                      val url = connectionRefOptions("url")
                      val expectationsResult = runExpectations(conn)
                      val keepGoing =
                        expectationsResult.isSuccess || !settings.appConfig.expectations.failOnError
                      if (keepGoing) {
                        // is it a new table ?
                        val targetTableExists = JdbcDbUtils.tableExists(conn, url, targetTable)

                        // alter target schema if needed
                        if (targetTableExists) {
                          // update target table schema if needed
                          logger.info(
                            s"Schema ${domain.finalName}"
                          )
                          val existingSchema =
                            SparkUtils.getSchemaOption(conn, connectionRefOptions, targetTable)

                          val incomingSchema = schema.sparkSchemaWithoutIgnore(schemaHandler)
                          //                            datasetWithRenamedAttributes
                          //                              .drop(CometColumns.cometInputFileNameColumn)
                          //                              .schema
                          val incomingSchemaWithSCD2 =
                            if (isSCD2) {
                              incomingSchema
                                .add(
                                  StructField(
                                    strategy.start_ts
                                      .getOrElse(settings.appConfig.scd2StartTimestamp),
                                    TimestampType,
                                    nullable = true
                                  )
                                )
                                .add(
                                  StructField(
                                    strategy.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp),
                                    TimestampType,
                                    nullable = true
                                  )
                                )
                            } else
                              incomingSchema
                          val addedSchema =
                            SparkUtils.added(
                              incomingSchemaWithSCD2,
                              existingSchema.getOrElse(incomingSchema)
                            )
                          val deletedSchema =
                            SparkUtils.dropped(
                              incomingSchema,
                              existingSchema.getOrElse(incomingSchema)
                            )
                          val alterTableDropColumns =
                            SparkUtils.alterTableDropColumnsString(deletedSchema, targetTable)
                          if (alterTableDropColumns.nonEmpty) {
                            logger.info(
                              s"alter table $targetTable with ${alterTableDropColumns.size} columns to drop"
                            )
                            logger.debug(s"alter table ${alterTableDropColumns.mkString("\n")}")
                          }

                          val alterTableAddColumns =
                            SparkUtils.alterTableAddColumnsString(addedSchema, targetTable)
                          if (alterTableAddColumns.nonEmpty) {
                            logger.info(
                              s"alter table $targetTable with ${alterTableAddColumns.size} columns to add"
                            )
                            logger.debug(s"alter table ${alterTableAddColumns.mkString("\n")}")
                          }

                          alterTableDropColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
                          alterTableAddColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
                        }
                        // At this point if the table exists, it has the same schema as the dataframe
                        // And if it's a SCD2, it has the 2 extra timestamp columns

                        val secondStepResult =
                          applyJdbcSecondStep(
                            firstStepTempTable,
                            schema,
                            targetTableExists
                          )
                        secondStepResult

                      } else {
                        expectationsResult
                      }
                    }

                  case Failure(exception) =>
                    Failure(exception)
                }
              } else {
                firstStepResult
              }
            } match {
              case Failure(exception) =>
                ingestionJob.logLoadFailureInAudit(start, exception)
              case Success(result) =>
                result
            }
            if (twoSteps) {
              JdbcDbUtils.withJDBCConnection(connectionRefOptions) { conn =>
                JdbcDbUtils.dropTable(firstStepTempTable, conn)
              }
            }
            result
          case Failure(exception) =>
            ingestionJob.logLoadFailureInAudit(start, exception)
        }
    }
    // After each ingestionjob we explicitely clear the spark cache
    ingestionJob.session.catalog.clearCache()
    jobResult

  }

  private def applyJdbcSecondStep(
    firstStepTempTableName: String,
    starlakeSchema: Schema,
    targetTableExists: Boolean
  ): Try[JobResult] = {
    val engineName = mergedMetadata.getSink().getConnection().getJdbcEngineName()
    val fullTableName = s"${domain.finalName}.${schema.finalName}"
    val sourceUris = path.map(_.toString).mkString(",").replace("'", "\\'")
    val quote = settings.appConfig.jdbcEngines(engineName.toString.toLowerCase()).quote
    // postgres does not support merge / create __or replace__ table. We need to do it by hand
    val (outputTableName, fullOutputTableName) = (schema.finalName, fullTableName)

    val tempTable = firstStepTempTableName
    val targetTable = fullOutputTableName

    // Even if merge is able to handle data deletion, in order to have same behavior with spark
    // we require user to set dynamic partition overwrite
    // we have sql as optional because in dynamic partition overwrite mode, if no partition exists, we do nothing
    val sqlMerge =
      SQLUtils
        .buildJDBCSQLOnLoad(
          starlakeSchema,
          tempTable,
          targetTable,
          targetTableExists,
          mergedMetadata.getSink().getConnection().getJdbcEngineName(),
          Some(sourceUris),
          strategy,
          true
        )
    val sqlMerges = sqlMerge.replace("`", quote).splitSql()
    val (presql, mainSql) =
      if (sqlMerges.length == 1) {
        (Nil, sqlMerges.head)
      } else {
        (sqlMerges.dropRight(1).toList, sqlMerges.last)
      }
    val taskDesc = AutoTaskDesc(
      name = targetTable,
      presql = presql,
      sql = Some(mainSql),
      database = schemaHandler.getDatabase(domain),
      domain = domain.finalName,
      table = schema.finalName,
      write = Some(mergedMetadata.getWrite()),
      sink = mergedMetadata.sink,
      acl = schema.acl,
      comment = schema.comment,
      tags = schema.tags,
      parseSQL = Some(false)
    )
    val jobResult = AutoTask
      .task(taskDesc, Map.empty, None, engine = Engine.JDBC, truncate = false)(
        settings,
        storageHandler,
        schemaHandler
      )
      .run()
    jobResult
  }

  private def requireTwoSteps(schema: Schema, sink: JdbcSink): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    strategy.isMerge() ||
    schema.filter.nonEmpty ||
    settings.appConfig.archiveTable
  }
   */
}
