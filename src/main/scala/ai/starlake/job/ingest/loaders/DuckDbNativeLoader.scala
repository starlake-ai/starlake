package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.job.transform.JdbcAutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{IngestionCounters, SparkUtils}
import com.typesafe.scalalogging.StrictLogging
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import java.nio.charset.Charset
import scala.util.{Try, Using}

class DuckDbNativeLoader(ingestionJob: IngestionJob)(implicit
  val settings: Settings
) extends StrictLogging {

  val domain: Domain = ingestionJob.domain

  val schema: Schema = ingestionJob.schema

  val storageHandler: StorageHandler = ingestionJob.storageHandler

  val schemaHandler: SchemaHandler = ingestionJob.schemaHandler

  val path: List[Path] = ingestionJob.path

  val options: Map[String, String] = ingestionJob.options

  val strategy: WriteStrategy = ingestionJob.mergedMetadata.getStrategyOptions()

  lazy val mergedMetadata: Metadata = ingestionJob.mergedMetadata

  private def requireTwoSteps(schema: Schema): Boolean = {
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

  def run(): Try[IngestionCounters] = {
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
        val targetTableName = s"${domain.finalName}.${schema.finalName}"
        val sqlWithTransformedFields = schema.buildSqlSelectOnLoad(unionTempTables)

        val taskDesc = AutoTaskDesc(
          name = targetTableName,
          sql = Some(sqlWithTransformedFields),
          database = schemaHandler.getDatabase(domain),
          domain = domain.finalName,
          table = schema.finalName,
          presql = schema.presql,
          postsql = schema.postsql,
          sink = mergedMetadata.sink,
          rls = schema.rls,
          expectations = schema.expectations,
          acl = schema.acl,
          comment = schema.comment,
          tags = schema.tags,
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
          schema.targetSparkSchemaWithIgnoreAndScript(schemaHandler),
          targetTableName
        )

        // TODO archive if set
        tempTables.foreach { tempTable =>
          JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
            JdbcDbUtils.dropTable(conn, s"${domain.finalName}.$tempTable")
          }
        }
      } else {
        singleStepLoad(domain.finalName, schema.finalName, schemaWithMergedMetadata, path)
      }
    }.map { - =>
      IngestionCounters(-1, -1, -1)
    }
  }

  private def computeEffectiveInputSchema(): Schema = {
    mergedMetadata.resolveFormat() match {
      case Format.DSV =>
        (mergedMetadata.resolveWithHeader(), path.map(_.toString).headOption) match {
          case (java.lang.Boolean.TRUE, Some(sourceFile)) =>
            val csvHeaders = storageHandler.readAndExecute(
              new Path(sourceFile),
              Charset.forName(mergedMetadata.resolveEncoding())
            ) { is =>
              Using.resource(is) { reader =>
                assert(
                  mergedMetadata.resolveQuote().length <= 1,
                  "quote must be a single character"
                )
                assert(
                  mergedMetadata.resolveEscape().length <= 1,
                  "quote must be a single character"
                )
                val csvParserSettings = new CsvParserSettings()
                val format = new CsvFormat()
                format.setDelimiter(mergedMetadata.resolveSeparator())
                mergedMetadata.resolveQuote().headOption.foreach(format.setQuote)
                mergedMetadata.resolveEscape().headOption.foreach(format.setQuoteEscape)
                csvParserSettings.setFormat(format)
                // allocate twice the declared columns. If fail a strange exception is thrown: https://github.com/uniVocity/univocity-parsers/issues/247
                csvParserSettings.setMaxColumns(schema.attributes.length * 2)
                csvParserSettings.setNullValue(mergedMetadata.resolveNullValue())
                csvParserSettings.setHeaderExtractionEnabled(true)
                csvParserSettings.setMaxCharsPerColumn(-1)
                val csvParser = new CsvParser(csvParserSettings)
                csvParser.beginParsing(reader)
                // call this in order to get the headers even if there is no record
                csvParser.parseNextRecord()
                csvParser.getRecordMetadata.headers().toList
              }
            }
            val attributesMap = schema.attributes.map(attr => attr.name -> attr).toMap
            val csvAttributesInOrders =
              csvHeaders.map(h =>
                attributesMap.getOrElse(h, Attribute(h, ignore = Some(true), required = None))
              )
            // attributes not in csv input file must not be required but we don't force them to optional.
            val effectiveAttributes =
              csvAttributesInOrders ++ schema.attributes.diff(csvAttributesInOrders)
            if (effectiveAttributes.length > schema.attributes.length) {
              logger.warn(
                s"Attributes in the CSV file are bigger from the schema. " +
                s"Schema will be updated to match the CSV file. " +
                s"Schema: ${schema.attributes.map(_.name).mkString(",")}. " +
                s"CSV: ${csvHeaders.mkString(",")}"
              )
              schema.copy(attributes = effectiveAttributes.take(schema.attributes.length))

            } else {
              schema.copy(attributes = effectiveAttributes)
            }

          case _ => schema
        }
      case _ => schema
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
