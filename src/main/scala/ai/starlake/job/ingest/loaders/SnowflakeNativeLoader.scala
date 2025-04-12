package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.job.transform.JdbcAutoTask
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{IngestionCounters, SparkUtils}
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import java.nio.charset.Charset
import scala.util.{Try, Using}

class SnowflakeNativeLoader(ingestionJob: IngestionJob)(implicit settings: Settings)
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
      JdbcDbUtils
        .withJDBCConnection(sinkConnection.options) { conn =>
          if (twoSteps) {
            val tempTables =
              path.map { p =>
                logger.info(s"Loading $p to temporary table")
                val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
                singleStepLoad(
                  domain.finalName,
                  tempTable,
                  schemaWithMergedMetadata,
                  List(p),
                  temporary = true,
                  conn
                )
                val filenameSQL =
                  s"ALTER TABLE ${domain.finalName}.$tempTable ADD COLUMN ${CometColumns.cometInputFileNameColumn} STRING DEFAULT '$p';"

                JdbcDbUtils.execute(filenameSQL, conn)

                tempTable
              }

            val unionTempTables =
              tempTables.map("SELECT * FROM " + _).mkString("(", " UNION ALL ", ")")
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
            job.runJDBC(None, Some(conn))
            job.updateJdbcTableSchema(
              starlakeSchema.targetSparkSchemaWithIgnoreAndScript(schemaHandler),
              targetTableName
            )

            // TODO archive if set
            tempTables.foreach { tempTable =>
              JdbcDbUtils.dropTable(conn, s"${domain.finalName}.$tempTable")
            }
          } else {
            singleStepLoad(
              domain.finalName,
              starlakeSchema.finalName,
              schemaWithMergedMetadata,
              path,
              temporary = false,
              conn
            )
          }
        }
    }.map { - =>
      List(IngestionCounters(-1, -1, -1, path.map(_.toString)))
    }

  }

  def copyExtraOptions(commonOptions: List[String]): String = {
    var extraOptions = ""
    if (mergedMetadata.getOptions().nonEmpty) {
      mergedMetadata.getOptions().foreach { case (k, v) =>
        if (!commonOptions.contains(k)) {
          extraOptions += s"$k = $v\n"
        }
      }
    }
    extraOptions
  }
  private val skipCount = {
    if (
      mergedMetadata.getOptions().get("SKIP_HEADER").isEmpty && mergedMetadata.resolveWithHeader()
    ) {
      Some("1")
    } else {
      mergedMetadata.getOptions().get("SKIP_HEADER")
    }
  }

  val pattern = this.starlakeSchema.pattern.pattern()

  private val (compressionFormat, extension): (String, String) = {
    val compression =
      mergedMetadata.getOptions().getOrElse("COMPRESSION", "true").equalsIgnoreCase("true")
    if (compression)
      ("COMPRESSION = GZIP", ".gz")
    else
      ("COMPRESSION = NONE", "")
  }

  private val nullIf: String = mergedMetadata.getOptions().get("NULL_IF") match {
    case Some(value) if value.nonEmpty =>
      s"NULL_IF = $value"
    case None =>
      if (mergedMetadata.resolveEmptyIsNull())
        "NULL_IF = ('')"
      else
        ""
  }

  private def purge: String =
    if (mergedMetadata.getOptions().getOrElse("PURGE", "false").equalsIgnoreCase("true"))
      "TRUE"
    else
      "FALSE"

  def encoding =
    mergedMetadata
      .getOptions()
      .getOrElse("ENCODING", mergedMetadata.resolveEncoding())

  private def buildCopyJson(domainAndTableName: String) = {
    val stripOuterArray = mergedMetadata
      .getOptions()
      .getOrElse("STRIP_OUTER_ARRAY", mergedMetadata.resolveArray().toString.toUpperCase())
    val commonOptions = List("STRIP_OUTER_ARRAY", "NULL_IF")
    val matchByColumnName =
      if (
        starlakeSchema.attributes.exists(
          _.primitiveType(schemaHandler).getOrElse(PrimitiveType.string) ==
            PrimitiveType.variant
        )
      )
        ""
      else
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"

    val extraOptions = copyExtraOptions(commonOptions)
    val sql =
      s"""
         |COPY INTO $domainAndTableName
         |FROM @$tempStage/${domain.finalName}/
         |PATTERN = '$pattern'
         |PURGE = ${purge}
         |FILE_FORMAT = (
         |  TYPE = JSON
         |  STRIP_OUTER_ARRAY = $stripOuterArray
         |  $nullIf
         |  $extraOptions
         |  $compressionFormat
         |)
         |$matchByColumnName
         |""".stripMargin
    sql
  }
  private def buildCopyOther(domainAndTableName: String, format: String) = {
    val commonOptions = List("NULL_IF")
    val extraOptions = copyExtraOptions(commonOptions)
    val sql =
      s"""
         |COPY INTO $domainAndTableName
         |FROM @$tempStage/${domain.finalName}/
         |PATTERN = '$pattern'
         |PURGE = $purge
         |FILE_FORMAT = (
         |  TYPE = $format
         |  $nullIf
         |  $extraOptions
         |  $compressionFormat
         |)
         |""".stripMargin
    sql
  }

  private def buildCopyCsv(domainAndTableName: String): String = {
    val commonOptions = List(
      "SKIP_HEADER",
      "NULL_IF",
      "FIELD_OPTIONALLY_ENCLOSED_BY",
      "FIELD_DELIMITER",
      "ESCAPE_UNENCLOSED_FIELD",
      "ENCODING"
    )
    val extraOptions = copyExtraOptions(commonOptions)

    val fieldOptionallyEnclosedBy =
      mergedMetadata
        .getOptions()
        .getOrElse("FIELD_OPTIONALLY_ENCLOSED_BY", mergedMetadata.resolveQuote())
    val separator =
      mergedMetadata
        .getOptions()
        .getOrElse("FIELD_DELIMITER", mergedMetadata.resolveSeparator())

    /*
    	First argument "\\\\": represents a single backslash in regex.
	    Second argument "\\\\\\\\": each \\ is a single backslash in the result, so this inserts two backslashes.

     */
    val escapeUnEnclosedField =
      mergedMetadata
        .getOptions()
        .getOrElse("ESCAPE_UNENCLOSED_FIELD", mergedMetadata.resolveEscape())
        .replaceAll("\\\\", "\\\\\\\\")

    val sql =
      s"""
         |COPY INTO $domainAndTableName
         |FROM @$tempStage/${domain.finalName}/
         |PATTERN = '$pattern$extension'
         |PURGE = $purge
         |FILE_FORMAT = (
         |  TYPE = CSV
         |  ERROR_ON_COLUMN_COUNT_MISMATCH = false
         |  SKIP_HEADER = ${skipCount.getOrElse("")}
         |  FIELD_OPTIONALLY_ENCLOSED_BY = '$fieldOptionallyEnclosedBy'
         |  FIELD_DELIMITER = '$separator'
         |  ESCAPE_UNENCLOSED_FIELD = '$escapeUnEnclosedField'
         |  ENCODING = '$encoding'
         |  $nullIf
         |  $extraOptions
         |  $compressionFormat
         |)
         |""".stripMargin
    sql
  }
  def singleStepLoad(
    domain: String,
    table: String,
    schema: Schema,
    path: List[Path],
    temporary: Boolean,
    conn: java.sql.Connection
  ) = {
    val sinkConnection = mergedMetadata.getSinkConnection()
    val incomingSparkSchema = schema.targetSparkSchemaWithIgnoreAndScript(schemaHandler)
    val domainAndTableName = domain + "." + table
    val optionsWrite =
      new JdbcOptionsInWrite(sinkConnection.jdbcUrl, domainAndTableName, sinkConnection.options)
    val ddlMap = schemaHandler.getDdlMapping(schema)
    val attrsWithDDLTypes = schemaHandler.getAttributesWithDDLType(schema, "snowflake")

    // Create or update table schema first
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
            temporaryTable = temporary,
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
          temporaryTable = temporary,
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
    val pathsAsString =
      path
        .map { p =>
          val ps = StorageHandler.localFile(p).pathAsString
          "file://" + ps
        }
    var res = JdbcDbUtils.executeQueryAsTable(s"USE SCHEMA $domain", conn)
    logger.info(res.toString())
    res = JdbcDbUtils.executeQueryAsTable(s"CREATE OR REPLACE TEMPORARY STAGE $tempStage", conn)
    logger.info(res.toString())
    val putSqls = pathsAsString.map(path => s"PUT $path @$tempStage/$domain")
    putSqls.map { putSql =>
      res = JdbcDbUtils.executeQueryAsTable(putSql, conn)
      logger.info(res.toString())
      res
    }
    mergedMetadata.resolveFormat() match {
      case Format.DSV =>
        val sql = buildCopyCsv(domainAndTableName)
        JdbcDbUtils.executeQueryAsTable(sql, conn)

      case Format.JSON_FLAT | Format.JSON =>
        val sql = buildCopyJson(domainAndTableName)
        JdbcDbUtils.executeQueryAsTable(sql, conn)
      case _ =>
    }

  }
}
