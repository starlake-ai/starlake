package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.job.transform.JdbcAutoTask
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{IngestionCounters, SparkUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import scala.util.Try

class DuckDbNativeLoader(ingestionJob: IngestionJob)(implicit settings: Settings)
    extends NativeLoader(ingestionJob, None) {

  override protected def requireTwoSteps(schema: Schema): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    strategy.isMerge() ||
    !schema.isVariant() ||
    schema.filter.nonEmpty ||
    settings.appConfig.archiveTable
  }
  lazy val effectiveSchema: Schema = computeEffectiveInputSchema()
  lazy val schemaWithMergedMetadata = effectiveSchema.copy(metadata = Some(mergedMetadata))

  def run(): Try[List[IngestionCounters]] = {
    Try {
      val sinkConnection = mergedMetadata.getSinkConnection()
      val twoSteps = requireTwoSteps(effectiveSchema)
      if (twoSteps) {
        val tempTables =
          path.map { p =>
            logger.info(s"Loading $p to temporary table")
            val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
            singleStepLoad(domain.finalName, tempTable, schemaWithMergedMetadata, List(p))
            val filenameSQL =
              s"ALTER TABLE ${domain.finalName}.$tempTable ADD COLUMN ${CometColumns.cometInputFileNameColumn} STRING DEFAULT '$p';"

            JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
              JdbcDbUtils.execute(filenameSQL, conn)

            }
            tempTable
          }

        val unionTempTables = tempTables.map("SELECT * FROM " + _).mkString("(", " UNION ALL ", ")")
        val targetTableName = s"${domain.finalName}.${starlakeSchema.finalName}"
        val sqlWithTransformedFields = starlakeSchema.buildSqlSelectOnLoad(unionTempTables)

        val taskDesc = AutoTaskDesc(
          name = targetTableName,
          sql = Some(sqlWithTransformedFields),
          database = schemaHandler.getDatabase(domain),
          domain = domain.finalName,
          table = starlakeSchema.finalName,
          presql = starlakeSchema.presql,
          postsql = starlakeSchema.postsql,
          sink = mergedMetadata.sink,
          rls = starlakeSchema.rls,
          expectations = starlakeSchema.expectations,
          acl = starlakeSchema.acl,
          comment = starlakeSchema.comment,
          tags = starlakeSchema.tags,
          writeStrategy = mergedMetadata.writeStrategy,
          parseSQL = Some(true),
          connectionRef = Option(mergedMetadata.getSinkConnectionRef())
        )

        val job =
          new JdbcAutoTask(
            Option(ingestionJob.applicationId()),
            taskDesc,
            Map.empty,
            None,
            truncate = false,
            test = false,
            logExecution = true
          )(
            settings,
            storageHandler,
            schemaHandler
          )
        job.run()
        job.updateJdbcTableSchema(
          starlakeSchema.targetSparkSchemaWithIgnoreAndScript(schemaHandler),
          targetTableName
        )

        // TODO archive if set
        tempTables.foreach { tempTable =>
          JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
            JdbcDbUtils.dropTable(conn, s"${domain.finalName}.$tempTable")
          }
        }
      } else {
        singleStepLoad(domain.finalName, starlakeSchema.finalName, schemaWithMergedMetadata, path)
      }
    }.map { - =>
      List(IngestionCounters(-1, -1, -1, path.map(_.toString)))
    }
  }

  def singleStepLoad(domain: String, table: String, schema: Schema, path: List[Path]) = {
    val sinkConnection = mergedMetadata.getSinkConnection()
    val incomingSparkSchema = schema.targetSparkSchemaWithIgnoreAndScript(schemaHandler)
    val domainAndTableName = domain + "." + table
    val optionsWrite =
      new JdbcOptionsInWrite(sinkConnection.jdbcUrl, domainAndTableName, sinkConnection.options)
    val ddlMap = schemaHandler.getDdlMapping(schema)
    val attrsWithDDLTypes = schemaHandler.getAttributesWithDDLType(schema, "duckdb")

    // Create or update table schema first
    JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
      val stmtExternal = conn.createStatement()
      stmtExternal.close()
      val tableExists = JdbcDbUtils.tableExists(conn, sinkConnection.jdbcUrl, domainAndTableName)
      JdbcDbUtils.createSchema(conn, domain)
      strategy.getEffectiveType() match {
        case WriteStrategyType.APPEND =>
          if (tableExists) {
            SparkUtils.updateJdbcTableSchema(
              conn,
              sinkConnection.options,
              domainAndTableName,
              incomingSparkSchema,
              attrsWithDDLTypes
            )
          } else {
            SparkUtils.createTable(
              conn,
              domainAndTableName,
              incomingSparkSchema,
              caseSensitive = true,
              temporaryTable = false,
              optionsWrite,
              ddlMap
            )
          }
        case _ => //  WriteStrategyType.OVERWRITE or first step of other strategies
          JdbcDbUtils.dropTable(conn, domainAndTableName)
          SparkUtils.createTable(
            conn,
            domainAndTableName,
            incomingSparkSchema,
            caseSensitive = true,
            temporaryTable = false,
            optionsWrite,
            ddlMap
          )
      }
      val columnsString =
        attrsWithDDLTypes
          .map { case (attr, ddlType) =>
            s"'$attr': '$ddlType'"
          }
          .mkString(", ")
      val paths =
        path
          .map { p =>
            val ps = p.toString
            if (ps.startsWith("file:"))
              if (ps.startsWith("file:"))
                StorageHandler.localFile(p).pathAsString
              else if (ps.contains { "://" }) {
                val defaultEndpoint =
                  ps.substring(2) match {
                    case "gs" => "storage.googleapis.com"
                    case "s3" => "s3.amazonaws.com"
                    case _    => "s3.amazonaws.com"
                  }
                val endpoint =
                  sinkConnection.options.getOrElse("s3_endpoint", defaultEndpoint)
                val keyid =
                  sinkConnection.options("s3_access_key_id")
                val secret =
                  sinkConnection.options("s3_secret_access_key")
                JdbcDbUtils.execute("INSTALL httpfs;", conn)
                JdbcDbUtils.execute("LOAD httpfs;", conn)
                JdbcDbUtils.execute(s"SET s3_endpoint='$endpoint';", conn)
                JdbcDbUtils.execute(s"SET s3_access_key_id='$keyid';", conn)
                JdbcDbUtils.execute(s"SET s3_secret_access_key='$secret';", conn)
                ps
              } else {
                ps
              }
          }
          .mkString("['", "','", "']")
      mergedMetadata.resolveFormat() match {
        case Format.DSV =>
          val nullstr =
            if (Option(mergedMetadata.resolveNullValue()).isEmpty)
              ""
            else
              s"nullstr = '${mergedMetadata.resolveNullValue()}',"
          val options = mergedMetadata.getOptions()
          val extraOptions =
            if (options.nonEmpty)
              options
                .map { case (k, v) =>
                  s"$k = '$v'"
                }
                .mkString("", ",", ",")
            else
              ""

          val sql = s"""INSERT INTO $domainAndTableName SELECT
               | * FROM read_csv(
               | ${paths},
               | delim = '${mergedMetadata.resolveSeparator()}',
               | header = ${mergedMetadata.resolveWithHeader()},
               | quote = '${mergedMetadata.resolveQuote()}',
               | escape = '${mergedMetadata.resolveEscape()}',
               | $nullstr
               | $extraOptions
               | columns = { $columnsString});""".stripMargin
          JdbcDbUtils.execute(sql, conn)

        case Format.JSON_FLAT | Format.JSON =>
          val format =
            if (mergedMetadata.resolveArray()) "array"
            else if (mergedMetadata.resolveMultiline())
              "unstructured"
            else
              "newline_delimited"
          if (schema.isFlat()) {
            val sql =
              s"""INSERT INTO  $domainAndTableName SELECT * FROM read_json($paths, format = '$format', columns = { $columnsString});"""
            JdbcDbUtils.execute(sql, conn)
          } else {
            schema.attributes.head.primitiveType(schemaHandler) match {
              case Some(PrimitiveType.variant) =>
                val sql =
                  s"""INSERT INTO $domainAndTableName SELECT * FROM read_json_objects($paths, format = '$format');"""
                JdbcDbUtils.execute(sql, conn)
              case _ =>
                s"""INSERT INTO $domainAndTableName SELECT * FROM read_json($paths, auto_detect = true, format = '$format');"""
            }
          }
        case _ =>
      }
    }
  }
}
