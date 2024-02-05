package ai.starlake.sql

import ai.starlake.config.Settings
import ai.starlake.config.Settings.Connection
import ai.starlake.schema.model.Schema.SL_INTERNAL_TABLE
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import net.sf.jsqlparser.parser.{CCJSqlParser, CCJSqlParserUtil}
import net.sf.jsqlparser.statement.select.{
  PlainSelect,
  Select,
  SelectVisitorAdapter,
  SetOperationList
}
import net.sf.jsqlparser.statement.{Statement, StatementVisitorAdapter}
import net.sf.jsqlparser.util.TablesNamesFinder

import java.util.UUID
import java.util.function.Consumer
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaSetConverter}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object SQLUtils extends StrictLogging {
  val fromsRegex = "(?i)\\s+FROM\\s+([_\\-a-z0-9`./(]+\\s*[ _,a-z0-9`./(]*)".r
  val joinRegex = "(?i)\\s+JOIN\\s+([_\\-a-z0-9`./]+)".r

  /** Syntax parser
    *
    * identifier = X | X.Y.Z | `X` | `X.Y.Z` | `X`.Y.Z
    *
    * FROM identifier
    *
    * FROM parquet.`/path-to/file`
    *
    * FROM
    *
    * JOIN identifier
    */
  //
  def extractRefsInFromAndJoin(sql: String): List[String] = {
    val froms =
      fromsRegex
        .findAllMatchIn(sql)
        .map(_.group(1))
        .toList
        .flatMap(_.split(",").map(_.trim))
        .map {
          // because the regex above is not powerful enough
          table =>
            val space = table.replaceAll("\\s", " ").indexOf(' ')
            if (space > 0)
              table.substring(0, space)
            else
              table
        }
        .filter(!_.contains("(")) // we remove constructions like 'year from date(...)'

    val joins = joinRegex.findAllMatchIn(sql).map(_.group(1)).toList
    // val ctes = cteRegex.findAllMatchIn(sql).map(_.group(1)).toList
    // val cteRegex = "(?i)\\s+WITH\\s+([_\\-a-z0-9`./(]+\\s*[ _,a-z0-9`./(]*)".r
    // val cteNameRegex = "(?i)\\s+([a-z0-9]+)+\\s+AS\\s*\\(".r

    // def extractCTEsFromSQL(sql: String): List[String] = {
    //  val ctes = cteRegex.findAllMatchIn(sql).map(_.group(1)).toList
    //  ctes.map(_.replaceAll("`", ""))
    // }

    (froms ++ joins).map(_.replaceAll("`", ""))
  }

  def extractRefsInFromCTEs(sql: String): List[String] = {
    val cteRegex = "(?i)\\s+WITH\\s+([_\\-a-z0-9`./(]+\\s*[ _,a-z0-9`./(]*)".r
    val ctes = cteRegex.findAllMatchIn(sql).map(_.group(1)).toList
    ctes
  }

  def extractColumnNames(sql: String): List[String] = {
    var result: List[String] = Nil
    def extractColumnsFromPlainSelect(plainSelect: PlainSelect): Unit = {
      val selectItems = Option(plainSelect.getSelectItems).map(_.asScala).getOrElse(Nil)
      result = selectItems.map { selectItem =>
        selectItem.getASTNode.jjtGetLastToken().image
      }.toList
    }
    val selectVisitorAdapter = new SelectVisitorAdapter() {
      override def visit(plainSelect: PlainSelect): Unit = {
        extractColumnsFromPlainSelect(plainSelect)
      }
      override def visit(setOpList: SetOperationList): Unit = {
        val plainSelect = setOpList.getSelect(0).getPlainSelect()
        extractColumnsFromPlainSelect(plainSelect)
      }
    }
    val statementVisitor = new StatementVisitorAdapter() {
      override def visit(select: Select): Unit = {
        select.accept(selectVisitorAdapter)
      }
    }
    val select = jsqlParse(sql)
    select.accept(statementVisitor)
    result
  }

  def extractTableNames(sql: String): List[String] = {
    val select = jsqlParse(sql)
    val finder = new TablesNamesFinder()
    val tableList = Option(finder.getTables(select)).map(_.asScala).getOrElse(Nil)
    tableList.toList
  }

  def extractCTENames(sql: String): List[String] = {
    var result: ListBuffer[String] = ListBuffer()
    val statementVisitor = new StatementVisitorAdapter() {
      override def visit(select: Select): Unit = {
        val ctes = Option(select.getWithItemsList()).map(_.asScala).getOrElse(Nil)
        ctes.foreach { withItem =>
          val alias = Option(withItem.getAlias).map(_.getName).getOrElse("")
          if (alias.nonEmpty)
            result += alias
        }
      }
    }
    val select = jsqlParse(sql)
    select.accept(statementVisitor)
    result.toList
  }

  def jsqlParse(sql: String): Statement = {
    val parseable =
      sql
        .replaceAll("(?i)WHEN NOT MATCHED AND (.*) THEN ", "WHEN NOT MATCHED THEN ")
        .replaceAll("(?i)WHEN MATCHED (.*) THEN ", "WHEN MATCHED THEN ")
    val features = new Consumer[CCJSqlParser] {
      override def accept(t: CCJSqlParser): Unit = {
        t.withTimeOut(60 * 1000)
      }
    }
    CCJSqlParserUtil.parse(parseable, features)
  }

  def getColumnNames(sql: String): String = {
    val columnNames = SQLUtils.extractColumnNames(sql)
    val columnNamesString = columnNames.mkString("(", ",", ")")
    columnNamesString
  }

  private def whenNotMatchedInsert(sql: String): String = {
    val columnNamesString = getColumnNames(sql)
    s"""WHEN NOT MATCHED THEN INSERT $columnNamesString VALUES $columnNamesString"""
  }

  private def whenMatchedUpdate(sql: String) = {
    val columnNames = SQLUtils.extractColumnNames(sql)
    columnNames
      .map { colName =>
        s"""$colName = SL_INTERNAL_SOURCE.$colName"""
      }
      .mkString("WHEN MATCHED THEN UPDATE SET ", ", ", "")
  }

  /** Build a merge SQL statement from a select statement without timestamp condition This relies on
    * the engine name where preactions and canMerge determine how the request should be generated
    *
    * @param sql
    * @param key
    * @param database
    * @param domain
    * @param targetTable
    * @param connection
    * @param isFilesystem
    * @param settings
    * @return
    */
  def buildMergeSqlOnTransform(
    sql: String,
    mergeOptions: StrategyOptions,
    database: Option[String],
    domain: String,
    targetTable: String,
    connection: Connection,
    targetTableExists: Boolean
  )(implicit settings: Settings): String = {
    val fullTargetTableName = database match {
      case Some(db) =>
        OutputRef(db, domain, targetTable).toSQLString(connection)
      case None =>
        OutputRef("", domain, targetTable).toSQLString(connection)
    }
    val columnNames = extractColumnNames(sql)

    buildJdbcSqlMerge(
      Right(sql),
      fullTargetTableName,
      targetTableExists,
      mergeOptions,
      columnNames,
      connection.getJdbcEngineName()
    )
  }

  def buildSingleSQLQueryOnTransform(
    sql: String,
    refs: Refs,
    domains: List[Domain],
    tasks: List[AutoTaskDesc],
    connection: Connection
  )(implicit
    settings: Settings
  ): String = {
    logger.info(s"Source SQL: $sql")
    val fromResolved =
      buildSingleSQLQueryForRegex(
        sql,
        refs,
        domains,
        tasks,
        SQLUtils.fromsRegex,
        "FROM",
        connection
      )
    logger.info(s"fromResolved SQL: $fromResolved")
    val joinAndFromResolved =
      buildSingleSQLQueryForRegex(
        fromResolved,
        refs,
        domains,
        tasks,
        SQLUtils.joinRegex,
        "JOIN",
        connection
      )
    logger.info(s"joinAndFromResolved SQL: $joinAndFromResolved")
    joinAndFromResolved
  }

  def buildSingleSQLQueryForRegex(
    sql: String,
    refs: Refs,
    domains: List[Domain],
    tasks: List[AutoTaskDesc],
    fromOrJoinRegex: Regex,
    keyword: String,
    connection: Connection
  )(implicit
    settings: Settings
  ): String = {
    val ctes = SQLUtils.extractCTENames(sql)
    var resolvedSQL = ""
    var startIndex = 0
    val fromMatches = fromOrJoinRegex
      .findAllMatchIn(sql)
      .toList
    if (fromMatches.isEmpty) {
      sql
    } else {
      val tablesList = this.extractTableNames(sql)
      fromMatches
        .foreach { regex =>
          var source = regex.source.toString.substring(regex.start, regex.end)

          val tablesFound =
            source
              .substring(
                source.toUpperCase().indexOf(keyword.toUpperCase()) + keyword.length
              ) // SKIP FROM AND JOIN
              .split(",")
              .map(_.trim.split("\\s").head) // get expressions in FROM / JOIN
              .filter(tablesList.contains(_)) // we remove CTEs
              .sortBy(_.length) // longer tables first because we need to make sure
              .reverse // that we are replacing the whole word

          tablesFound.foreach { tableFound =>
            val resolvedTableName = resolveTableNameInSql(
              tableFound,
              refs,
              domains,
              tasks,
              ctes,
              connection
            )
            source = source.replaceAll(tableFound, resolvedTableName)
          }
          resolvedSQL += sql.substring(startIndex, regex.start) + source
          startIndex = regex.end
        }
      resolvedSQL = resolvedSQL + sql.substring(startIndex)
      resolvedSQL
    }
  }

  private def resolveTableNameInSql(
    tableName: String,
    refs: Refs,
    domains: List[Domain],
    tasks: List[AutoTaskDesc],
    ctes: List[String],
    connection: Connection
  )(implicit
    settings: Settings
  ): String = {
    def cteContains(table: String): Boolean = ctes.exists(cte => cte.equalsIgnoreCase(table))
    if (tableName.contains('/')) {
      // This is a file in the form of parquet.`/path/to/file`
      tableName
    } else {
      val quoteFreeTableName = List("\"", "`", "'").foldLeft(tableName) { (tableName, quote) =>
        tableName.replaceAll(quote, "")
      }
      val tableTuple = quoteFreeTableName.split("\\.").toList

      // We need to find it in the refs
      val activeEnvRefs = refs
      val databaseDomainTableRef =
        activeEnvRefs
          .getOutputRef(tableTuple)
          .map(_.toSQLString(connection))
      val resolvedTableName = databaseDomainTableRef.getOrElse {
        resolveTableRefInDomainsAndJobs(tableTuple, domains, tasks) match {
          case Success((database, domain, table)) =>
            ai.starlake.schema.model
              .OutputRef(database, domain, table)
              .toSQLString(connection)
          case Failure(e) =>
            Utils.logException(logger, e)
            throw e
        }
      }
      resolvedTableName
    }
  }

  private def resolveTableRefInDomainsAndJobs(
    tableComponents: List[String],
    domains: List[Domain],
    tasks: List[AutoTaskDesc]
  )(implicit
    settings: Settings
  ): Try[(String, String, String)] = Try {
    val (database, domain, table): (Option[String], Option[String], String) =
      tableComponents match {
        case table :: Nil =>
          (None, None, table)
        case domain :: table :: Nil =>
          (None, Some(domain), table)
        case database :: domain :: table :: Nil =>
          (Some(database), Some(domain), table)
        case _ =>
          throw new Exception(
            s"Invalid table reference ${tableComponents.mkString(".")}"
          )
      }
    (database, domain, table) match {
      case (Some(db), Some(dom), table) =>
        (db, dom, table)
      case (None, domainComponent, table) =>
        val domainsByFinalName = domains
          .filter { dom =>
            val domainOK = domainComponent.forall(_.equalsIgnoreCase(dom.finalName))
            domainOK && dom.tables.exists(_.finalName.equalsIgnoreCase(table))
          }
        val tasksByTable =
          tasks.find { task =>
            val domainOK = domainComponent.forall(_.equalsIgnoreCase(task.domain))
            domainOK && task.table.equalsIgnoreCase(table)
          }.toList

        val nameCountMatch =
          domainsByFinalName.length + tasksByTable.length
        val (database, domain) = if (nameCountMatch > 1) {
          val domainNames = domainsByFinalName.map(_.finalName).mkString(",")
          logger.error(s"Table $table is present in domain(s): $domainNames.")

          val taskNamesByTable = tasksByTable.map(_.table).mkString(",")
          logger.error(s"Table $table is present as a table in tasks(s): $taskNamesByTable.")

          val taskNames = tasksByTable.map(_.name).mkString(",")
          logger.error(s"Table $table is present as a table in tasks(s): $taskNames.")
          throw new Exception("Table is present in multiple domains and/or tasks")
        } else if (nameCountMatch == 1) {
          domainsByFinalName.headOption
            .map(dom => (dom.database, dom.finalName))
            .orElse(tasksByTable.headOption.map { task =>
              (task.database, task.domain)
            })
            .getOrElse((None, ""))
        } else { // nameCountMatch == 0
          logger.info(s"Table $table not found in any domain or task; This is probably a CTE")
          (None, domainComponent.getOrElse(""))
        }
        val databaseName = database
          .orElse(settings.appConfig.getDefaultDatabase())
          .getOrElse("")
        (databaseName, domain, table)
      case _ =>
        throw new Exception(
          s"Invalid table reference ${tableComponents.mkString(".")}"
        )
    }

  }

  def temporaryTableName(tableName: String): String =
    "zztmp_" + tableName + "_" + UUID.randomUUID().toString.replace("-", "")

  def stripComments(sql: String): String = {

    // Remove single line comments
    val sql1 = sql.split("\n").map(_.replaceAll("--.*$", "")).mkString("\n")
    // Remove multi-line comments
    val sql2 = sql1.replaceAll("(?s)/\\*.*?\\*/", "")
    sql2.trim
  }

  def buildJDBCSQLOnLoad(
    starlakeSchema: Schema,
    tempTable: String,
    targetTable: String,
    targetTableExists: Boolean,
    jdbcDatabase: Engine,
    sourceUris: Option[String],
    strategy: StrategyOptions,
    applyTransformAndIgnore: Boolean
  )(implicit settings: Settings): String = {
    val jdbcEngine = settings.appConfig.jdbcEngines(jdbcDatabase.toString)
    val quote = jdbcEngine.quote
    strategy.`type` match {
      case StrategyType.APPEND | StrategyType.OVERWRITE =>
        starlakeSchema.buildSqlSelectOnLoad(
          tempTable,
          sourceUris,
          quote,
          applyTransformAndIgnore = applyTransformAndIgnore
        )
      case _ =>
        val (scriptAttributes, transformAttributes) =
          starlakeSchema.scriptAndTransformAttributes().partition(_.script.nonEmpty)
        val simpleAttributes = starlakeSchema.exceptIgnoreScriptAndTransformAttributes()
        val allOutputAttributes = simpleAttributes ++ transformAttributes ++ scriptAttributes
        val allColumnNames = allOutputAttributes.map(_.getFinalName())
        val select = starlakeSchema.buildSqlSelectOnLoad(
          tempTable,
          sourceUris,
          quote,
          applyTransformAndIgnore = applyTransformAndIgnore
        )
        SQLUtils.buildJdbcSqlMerge(
          Right(select),
          targetTable,
          targetTableExists,
          starlakeSchema.getStrategy(),
          allColumnNames,
          jdbcDatabase
        )
    }
  }

  def buildJdbcSqlMerge(
    sourceTableOrStatement: Either[String, String],
    targetTable: String,
    targetTableExists: Boolean,
    strategy: StrategyOptions,
    targetTableColumns: List[String],
    jdbcDatabase: Engine
  )(implicit settings: Settings): String = {
    val jdbcEngine = settings.appConfig.jdbcEngines(jdbcDatabase.toString)
    val quote = jdbcEngine.quote
    val allAttributesSQL = targetTableColumns.mkString(",")
    val mergeKeys =
      strategy.key
        .map(key => s"$quote$key$quote")
        .mkString(",")

    val (targetColumns, sourceColumns) =
      targetTableColumns
        .map(col => s"$quote$col$quote" -> s"$SL_INTERNAL_TABLE.$quote$col$quote")
        .unzip
    val notMatchedInsertColumnsSql = targetColumns.mkString("(", ",", ")")
    val notMatchedInsertValuesSql = sourceColumns.mkString("(", ",", ")")
    val notMatchedInsertSql = s"""$notMatchedInsertColumnsSql VALUES $notMatchedInsertValuesSql"""
    val matchedUpdateSql = targetTableColumns
      .map(col => s"$col = $SL_INTERNAL_TABLE.$col")
      .mkString("SET ", ",", "")

    val mergeKeyJoinCondition =
      strategy.key.map(key => s"$SL_INTERNAL_TABLE.$key = $targetTable.$key").mkString(" AND ")

    val nullJoinCondition =
      strategy.key.map(key => s"$targetTable.$key IS NULL").mkString(" AND ")

    val mergeTimestampCol = strategy.timestamp
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)
    val canMerge = jdbcEngine.canMerge
    val startTsCol = strategy.start_ts.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTsCol = strategy.start_ts.getOrElse(settings.appConfig.scd2EndTimestamp)

    val (sourceTable, tempTable) = sourceTableOrStatement match {
      case Left(sourceTable) =>
        (sourceTable, Nil)
      case Right(sourceStatement) =>
        ("SL_SOURCE_TABLE", List(s"CREATE TEMPORARY TABLE SL_SOURCE_TABLE AS ($sourceStatement);"))
    }
    val result =
      strategy.`type` match {
        case StrategyType.UPSERT_BY_KEY =>
          buildJdbcSqlForMergeByKey(
            targetTable,
            targetTableExists,
            allAttributesSQL,
            mergeKeys,
            notMatchedInsertSql,
            matchedUpdateSql,
            mergeKeyJoinCondition,
            mergeOn,
            sourceTable
          )
        case StrategyType.UPSERT_BY_KEY_AND_TIMESTAMP =>
          buildJdbcSqlForMergeByKeyAndTimestamp(
            targetTable,
            targetTableExists,
            quote,
            allAttributesSQL,
            mergeKeys,
            sourceColumns,
            notMatchedInsertColumnsSql,
            notMatchedInsertSql,
            matchedUpdateSql,
            mergeKeyJoinCondition,
            nullJoinCondition,
            mergeTimestampCol,
            mergeOn,
            canMerge,
            sourceTable
          )
        case StrategyType.SCD2 =>
          buildJdbcSqlForSC2(
            targetTable,
            targetTableExists,
            strategy,
            quote,
            allAttributesSQL,
            mergeKeys,
            matchedUpdateSql,
            mergeKeyJoinCondition,
            nullJoinCondition,
            mergeTimestampCol,
            mergeOn,
            startTsCol,
            endTsCol,
            sourceTable
          )
        case unknownStrategy =>
          throw new Exception(s"Unknown strategy $unknownStrategy")
      }

    val extraPreActions = tempTable.mkString
    s"$extraPreActions\n$result"
  }

  private def buildJdbcSqlForSC2(
    targetTable: String,
    targetTableExists: Boolean,
    strategy: StrategyOptions,
    quote: String,
    allAttributesSQL: String,
    partitionKeys: String,
    matchedUpdateSql: String,
    joinCondition: String,
    nullJoinCondition: String,
    mergeTimestampCol: Option[String],
    mergeOn: MergeOn,
    startTsCol: String,
    endTsCol: String,
    sourceTable: String
  ) = {
    (targetTableExists, mergeTimestampCol, mergeOn) match {
      case (false, Some(_), MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        s"""
           |SELECT  $allAttributesSQL  FROM $sourceTable
            """.stripMargin
      case (false, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist
            We create a temporary table with a row number and select the first row for each partition based on the timestamp
            And then we insert the data
         */
        s"""
           |CREATE TEMPORARY TABLE SL_VIEW_WITH_ROWNUM AS
           |  SELECT  $allAttributesSQL,
           |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM $sourceTable;
           |SELECT  $allAttributesSQL  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1
            """.stripMargin

      case (true, Some(mergeTimestampCol), MergeOn.TARGET) =>
        /*
            First we insert all new rows
            Then we create a temporary table with the updated rows
            Then we update the end_ts of the old rows
            Then we insert the new rows
                INSERT INTO $targetTable
                SELECT $allAttributesSQL, $mergeTimestampCol AS $startTsCol, NULL AS $endTsCol FROM $sourceTable AS $SL_INTERNAL_TABLE
                WHERE $key IN (SELECT DISTINCT $key FROM SL_UPDATED_RECORDS);
         */
        val key = strategy.key.mkString(",")
        s"""
           |INSERT INTO $targetTable
           |SELECT $allAttributesSQL, NULL AS $startTsCol, NULL AS $endTsCol FROM $sourceTable AS $SL_INTERNAL_TABLE
           |LEFT JOIN $targetTable ON ($joinCondition AND $targetTable.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |CREATE TEMPORARY VIEW SL_UPDATED_RECORDS AS
           |SELECT $allAttributesSQL FROM $sourceTable AS $SL_INTERNAL_TABLE, $targetTable
           |WHERE $joinCondition AND $targetTable.$endTsCol IS NULL AND $SL_INTERNAL_TABLE.$mergeTimestampCol > $targetTable.$mergeTimestampCol;
           |
           |MERGE INTO $targetTable USING SL_UPDATED_RECORDS AS $SL_INTERNAL_TABLE ON ($joinCondition)
           |WHEN MATCHED THEN UPDATE $matchedUpdateSql, $startTsCol = $mergeTimestampCol, $endTsCol = NULL
           |""".stripMargin

      case (true, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        /*
            First we insert all new rows
            Then we create a temporary table with the updated rows
            Then we update the end_ts of the old rows
            Then we insert the new rows
                INSERT INTO $targetTable
                SELECT $allAttributesSQL, $mergeTimestampCol AS $startTsCol, NULL AS $endTsCol FROM $sourceTable AS $SL_INTERNAL_TABLE
                WHERE $key IN (SELECT DISTINCT $key FROM SL_UPDATED_RECORDS);
         */
        val key = strategy.key.mkString(",")
        s"""
           |INSERT INTO $targetTable
           |SELECT $allAttributesSQL, NULL AS $startTsCol, NULL AS $endTsCol FROM $sourceTable AS $SL_INTERNAL_TABLE
           |LEFT JOIN $targetTable ON ($joinCondition AND $targetTable.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |CREATE TEMPORARY TABLE SL_VIEW_WITH_ROWNUM AS
           |  SELECT  $allAttributesSQL,
           |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM $sourceTable;
           |CREATE TEMPORARY TABLE SL_DEDUP AS
           |SELECT  $allAttributesSQL  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1;
           |
           |CREATE TEMPORARY TABLE SL_UPDATED_RECORDS AS
           |SELECT $allAttributesSQL FROM SL_DEDUP AS $SL_INTERNAL_TABLE, $targetTable
           |WHERE $joinCondition AND $targetTable.$endTsCol IS NULL AND $SL_INTERNAL_TABLE.$mergeTimestampCol > $targetTable.$mergeTimestampCol;
           |
           |MERGE INTO $targetTable USING SL_UPDATED_RECORDS AS $SL_INTERNAL_TABLE ON ($joinCondition)
           |WHEN MATCHED THEN UPDATE $matchedUpdateSql, $startTsCol = $mergeTimestampCol, $endTsCol = NULL
           |""".stripMargin
      case (_, Some(_), MergeOn(_)) =>
        throw new Exception("Should never happen !!!")
      case (_, None, _) =>
        throw new Exception("SCD2 is not supported without a merge timestamp column")

    }
  }

  private def buildJdbcSqlForMergeByKeyAndTimestamp(
    targetTable: String,
    targetTableExists: Boolean,
    quote: String,
    allAttributesSQL: String,
    partitionKeys: String,
    sourceColumns: List[String],
    notMatchedInsertColumnsSql: String,
    notMatchedInsertSql: String,
    matchedUpdateSql: String,
    joinCondition: String,
    nullJoinCondition: String,
    mergeTimestampCol: Option[String],
    mergeOn: MergeOn,
    canMerge: Boolean,
    sourceTable: String
  ): String = {
    (targetTableExists, mergeTimestampCol, mergeOn) match {
      case (false, Some(_), MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        s"""SELECT  $allAttributesSQL  FROM $sourceTable"""

      case (false, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist
            We create a temporary table with a row number and select the first row for each partition based on the timestamp
            And then we insert the data
         */
        s"""
           |CREATE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
           |  SELECT  $allAttributesSQL,
           |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM $sourceTable;
           |SELECT  $allAttributesSQL FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1
            """.stripMargin

      case (true, Some(mergeTimestampCol), MergeOn.TARGET) =>
        /*
            The table exists, we can merge the data by joining on the key and comparing the timestamp
         */
        if (canMerge) {
          s"""
             |MERGE INTO $targetTable USING $sourceTable AS $SL_INTERNAL_TABLE ON ($joinCondition)
             |WHEN MATCHED AND $SL_INTERNAL_TABLE.$mergeTimestampCol > $targetTable.$mergeTimestampCol THEN UPDATE $matchedUpdateSql
             |WHEN NOT MATCHED THEN INSERT $notMatchedInsertSql
             |""".stripMargin
        } else {
          /*
                We cannot merge, we need to update the data first and then insert the new data
                The merge into comment instruct starlake runner to execute the request as it is.
           */
          s"""
             |UPDATE $targetTable $matchedUpdateSql
             |FROM $sourceTable AS $SL_INTERNAL_TABLE
             |WHERE $joinCondition AND $SL_INTERNAL_TABLE.$mergeTimestampCol > $targetTable.$mergeTimestampCol;
             |
             |/* merge into */
             |INSERT INTO $targetTable$notMatchedInsertColumnsSql
             |SELECT ${sourceColumns.mkString(",")} FROM $sourceTable AS $SL_INTERNAL_TABLE
             |LEFT JOIN $targetTable ON ($joinCondition)
             |WHERE $nullJoinCondition
             |
             |""".stripMargin
        }
      case (true, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        if (canMerge) {
          s"""
             |CREATE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
             |  SELECT  $allAttributesSQL,
             |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys  ORDER BY (select 0)) AS SL_SEQ
             |  FROM $sourceTable;
             |CREATE TEMPORARY VIEW SL_DEDUP AS SELECT  $allAttributesSQL  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1;
             |MERGE INTO $targetTable USING SL_DEDUP AS $SL_INTERNAL_TABLE ON ($joinCondition)
             |WHEN MATCHED AND $SL_INTERNAL_TABLE.$mergeTimestampCol > $targetTable.$mergeTimestampCol THEN UPDATE $matchedUpdateSql
             |WHEN NOT MATCHED THEN INSERT $notMatchedInsertSql
             |""".stripMargin
        } else {
          s"""
             |CREATE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
             |  SELECT  $allAttributesSQL,
             |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
             |  FROM $sourceTable;
             |CREATE TEMPORARY VIEW SL_DEDUP AS SELECT  $allAttributesSQL  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1;
             |
             |UPDATE $targetTable $matchedUpdateSql
             |FROM SL_DEDUP AS $SL_INTERNAL_TABLE
             |WHERE $joinCondition AND $SL_INTERNAL_TABLE.$mergeTimestampCol > $targetTable.$mergeTimestampCol;
             |
             |/* merge into */
             |INSERT INTO $targetTable$notMatchedInsertColumnsSql
             |SELECT ${sourceColumns.mkString(",")} FROM SL_DEDUP AS $SL_INTERNAL_TABLE
             |LEFT JOIN $targetTable ON ($joinCondition)
             |WHERE $nullJoinCondition
             |""".stripMargin
        }
      case (_, Some(_), MergeOn(_)) | (_, None, MergeOn(_)) =>
        throw new Exception("Should never happen !!!")
    }
  }

  private def buildJdbcSqlForMergeByKey(
    targetTable: String,
    targetTableExists: Boolean,
    allAttributesSQL: String,
    partitionKeys: String,
    notMatchedInsertSql: String,
    matchedUpdateSql: String,
    mergeKeyJoinCondition: String,
    mergeOn: MergeOn,
    sourceTable: String
  ) = {
    (targetTableExists, mergeOn) match {
      case (false, MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        s"""SELECT  $allAttributesSQL
           |FROM $sourceTable
            """.stripMargin
      case (false, MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist, but we are asked to deduplicate the data from teh input
            We create a temporary table with a row number and select the first row for each partition
            And then we insert the data
         */
        s"""
           |CREATE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
           |  SELECT  $allAttributesSQL,
           |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys ORDER BY (select 0)) AS SL_SEQ
           |  FROM $sourceTable;
           |SELECT  $allAttributesSQL  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1
            """.stripMargin
      case (true, MergeOn.TARGET) =>
        /*
            The table exists, we can merge the data
         */
        s"""
           |MERGE INTO $targetTable USING $sourceTable AS $SL_INTERNAL_TABLE ON ($mergeKeyJoinCondition)
           |WHEN MATCHED THEN UPDATE $matchedUpdateSql
           |WHEN NOT MATCHED THEN INSERT $notMatchedInsertSql
           |""".stripMargin

      case (true, MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table exists, We deduplicated the data from the input and we merge the data
         */
        s"""
           |CREATE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
           |  SELECT  $allAttributesSQL,
           |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys  ORDER BY (select 0)) AS SL_SEQ
           |  FROM $sourceTable;
           |CREATE TEMPORARY TABLE SL_DEDUP AS SELECT  $allAttributesSQL  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1;
           |MERGE INTO $targetTable USING SL_DEDUP AS $SL_INTERNAL_TABLE ON ($mergeKeyJoinCondition)
           |WHEN MATCHED THEN UPDATE $matchedUpdateSql
           |WHEN NOT MATCHED THEN INSERT $notMatchedInsertSql
           |""".stripMargin
      case (_, MergeOn(_)) =>
        throw new Exception("Should never happen !!!")

    }
  }

  def targetColumnsForSelectSql(targetTableColumns: List[String], quote: String): String =
    targetTableColumns.map(col => s"$quote$col$quote").mkString(",")

  def incomingColumnsForSelectSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String =
    targetTableColumns.map(col => s"$incomingTable.$quote$col$quote").mkString(",")

  def setForUpdateSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String =
    targetTableColumns
      .map(col => s"$quote$col$quote = $incomingTable.$quote$col$quote")
      .mkString("SET ", ",", "")

  def mergeKeyJoinCondition(
    incomingTable: String,
    targetTable: String,
    columns: List[String],
    quote: String
  ): String =
    columns
      .map(col => s"$incomingTable.$quote$col$quote = $targetTable.$quote$col$quote")
      .mkString(" AND ")

}
