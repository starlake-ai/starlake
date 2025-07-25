package ai.starlake.sql

import ai.starlake.config.Settings
import ai.starlake.config.Settings.Connection
import ai.starlake.schema.handlers.TableWithNameAndType
import ai.starlake.schema.model._
import ai.starlake.transpiler.{JSQLColumResolver, JSQLTranspiler}
import ai.starlake.utils.Utils
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging
import net.sf.jsqlparser.expression.Alias
import net.sf.jsqlparser.parser.{CCJSqlParser, CCJSqlParserUtil}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{
  PlainSelect,
  Select,
  SelectItem,
  SelectVisitorAdapter,
  SetOperationList
}
import net.sf.jsqlparser.statement.{Statement, StatementVisitorAdapter}
import net.sf.jsqlparser.util.TablesNamesFinder

import java.util.UUID
import java.util.function.Consumer
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object SQLUtils extends LazyLogging {
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
  def extractTableNamesUsingRegEx(sql: String): List[String] = {
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

    (froms ++ joins).map(_.replaceAll("`", "")).distinct
  }

  def extractTableNamesFromCTEsUsingRegEx(sql: String): List[String] = {
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

    val selectVisitorAdapter = new SelectVisitorAdapter[Any]() {
      override def visit[T](plainSelect: PlainSelect, context: T): Any = {
        extractColumnsFromPlainSelect(plainSelect)
      }

      override def visit[T](setOpList: SetOperationList, context: T): Any = {
        val plainSelect = setOpList.getSelect(0).getPlainSelect
        extractColumnsFromPlainSelect(plainSelect)
      }
    }
    val statementVisitor = new StatementVisitorAdapter[Any]() {
      override def visit[T](select: Select, context: T): Any = {
        select.accept(selectVisitorAdapter, null)
      }
    }
    val select = jsqlParse(sql)
    select.accept(statementVisitor, null)
    result
  }

  def extractTableNames(sql: String): List[String] = {
    val select = jsqlParse(sql)
    val finder = new TablesNamesFinder()
    val tableList = Option(finder.getTables(select)).map(_.asScala).getOrElse(Nil)
    tableList.toList.distinct
  }

  def extractCTENames(sql: String): List[String] = {
    var result: ListBuffer[String] = ListBuffer()
    val statementVisitor = new StatementVisitorAdapter[Any]() {
      override def visit[T](select: Select, context: T): Any = {
        val ctes = Option(select.getWithItemsList()).map(_.asScala).getOrElse(Nil)
        ctes.foreach { withItem =>
          val alias = Option(withItem.getAlias).map(_.getName).getOrElse("")
          if (alias.nonEmpty)
            result += alias
        }
        null
      }
    }
    val select = jsqlParse(sql)
    select.accept(statementVisitor, null)
    result.toList
  }

  private def jsqlParse(sql: String): Statement = {
    val features = new Consumer[CCJSqlParser] {
      override def accept(t: CCJSqlParser): Unit = {
        t.withTimeOut(60 * 1000)
      }
    }
    try {
      CCJSqlParserUtil.parse(sql, features)
    } catch {
      case exception: Exception =>
        logger.error(s"Failed to parse $sql")
        throw exception
    }
  }

  def substituteRefInSQLSelect(
    sql: String,
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
    connection: Connection
  )(implicit
    settings: Settings
  ): String = {
    logger.debug(s"Source SQL: $sql")
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
    logger.debug(s"fromResolved SQL: $fromResolved")
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
    logger.debug(s"joinAndFromResolved SQL: $joinAndFromResolved")
    joinAndFromResolved
  }

  def buildSingleSQLQueryForRegex(
    sql: String,
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
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
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
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
      val quoteFreeTName: String = quoteFreeTableName(tableName)
      val tableTuple = quoteFreeTName.split("\\.").toList

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

  def quoteFreeTableName(tableName: String) = {
    val quoteFreeTableName = List("\"", "`", "'").foldLeft(tableName) { (tableName, quote) =>
      tableName.replaceAll(quote, "")
    }
    quoteFreeTableName
  }

  private def resolveTableRefInDomainsAndJobs(
    tableComponents: List[String],
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo]
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

        // When the load and the transform go to the same table and in the same domain we do not take them into account
        val nameCountMatch = (domainsByFinalName.map(dom =>
          s"${dom.finalName}.$table".toLowerCase
        ) ++ tasksByTable.map(task => s"${task.domain}.$table".toLowerCase)).toSet.size

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
          logger.info(
            s"Table $table not found in any domain or task; This is probably a CTE or a temporary table"
          )
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

  def quoteCols(cols: List[String], quote: String): List[String] = {
    unquoteCols(cols, quote).map(col => s"${quote}$col${quote}")
  }

  def unquoteCols(cols: List[String], quote: String): List[String] = {
    cols.map { col =>
      if (quote.nonEmpty && col.startsWith(quote) && col.endsWith(quote))
        col.substring(1, col.length - 1)
      else
        col
    }
  }

  def unquoteAgressive(cols: List[String]): List[String] = {
    val quotes = List("\"", "'", "`")
    cols.map { col =>
      var result = col.trim
      quotes.foreach { quote =>
        if (result.startsWith(quote) && result.endsWith(quote)) {
          result = result.substring(1, result.length - 1)
        }
      }
      result
    }
  }

  def targetColumnsForSelectSql(targetTableColumns: List[String], quote: String): String =
    quoteCols(unquoteCols(targetTableColumns, quote), quote).mkString(",")

  def incomingColumnsForSelectSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String =
    unquoteCols(targetTableColumns, quote)
      .map(col => s"$incomingTable.$quote$col$quote")
      .mkString(",")

  def setForUpdateSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String =
    unquoteCols(targetTableColumns, quote)
      .map(col => s"$quote$col$quote = $incomingTable.$quote$col$quote")
      .mkString("SET ", ",", "")

  def mergeKeyJoinCondition(
    incomingTable: String,
    targetTable: String,
    columns: List[String],
    quote: String
  ): String =
    unquoteCols(columns, quote)
      .map(col => s"$incomingTable.$quote$col$quote = $targetTable.$quote$col$quote")
      .mkString(" AND ")

  def format(input: String, outputFormat: JSQLFormatter.OutputFormat): String = {
    val sql = input.trim
    val uppercaseSQL = sql.toUpperCase
    val formatted =
      if (
        uppercaseSQL.startsWith("SELECT") || uppercaseSQL.startsWith("WITH") || uppercaseSQL
          .startsWith("MERGE")
      ) {
        val preformat = sql.replaceAll("}}", "______\n").replaceAll("\\{\\{", "___\n")
        Try(
          JSQLFormatter.format(
            preformat,
            s"outputFormat=${outputFormat.name()}",
            "statementTerminator=NONE"
          )
        ).getOrElse(s"-- failed to format start\n$sql\n-- failed to format end")
      } else {
        sql
      }

    val result =
      if (formatted.startsWith("-- failed to format start")) {
        sql
      } else {
        val postFormat = formatted.replaceAll("______", "}}").replaceAll("___", "{{")
        if (outputFormat == JSQLFormatter.OutputFormat.HTML) {
          val startIndex = postFormat.indexOf("<body>") + "<body>".length
          val endIndex = postFormat.indexOf("</body>")
          if (startIndex > 0 && endIndex > 0) {
            postFormat.substring(startIndex, endIndex)
          } else {
            postFormat
          }
        } else {
          postFormat
        }
      }
    // remove extra ';' added by JSQLFormatter
    val trimmedResult = result.trim
    if (!sql.endsWith(";") && trimmedResult.endsWith(";")) {
      trimmedResult.substring(0, trimmedResult.length - 1)
    } else {
      result
    }
  }

  def transpilerDialect(conn: Connection): JSQLTranspiler.Dialect =
    conn._transpileDialect match {
      case Some(dialect) => JSQLTranspiler.Dialect.valueOf(dialect)
      case None =>
        if (conn.isSpark())
          JSQLTranspiler.Dialect.DATABRICKS
        if (conn.isBigQuery())
          JSQLTranspiler.Dialect.GOOGLE_BIG_QUERY
        else if (conn.isSnowflake())
          JSQLTranspiler.Dialect.SNOWFLAKE
        else if (conn.isRedshift())
          JSQLTranspiler.Dialect.AMAZON_REDSHIFT
        else if (conn.isDuckDb())
          JSQLTranspiler.Dialect.DUCK_DB
        else if (conn.isPostgreSql())
          JSQLTranspiler.Dialect.ANY
        else if (conn.isMySQLOrMariaDb())
          JSQLTranspiler.Dialect.ANY
        else if (conn.isJdbcUrl())
          JSQLTranspiler.Dialect.ANY
        else
          JSQLTranspiler.Dialect.ANY // Should not happen
    }

  def extractTableNamesWithTypes(
    sql: String
  )(implicit settings: Settings): List[(String, TableWithNameAndType)] = {
    val tables = SQLUtils.extractTableNamesUsingRegEx(
      Utils.parseJinja(sql, settings.schemaHandler().activeEnvVars())
    )
    val objects = settings.schemaHandler().objectsMap()
    tables.flatMap { t =>
      if (objects.contains(t)) {
        Some((t, objects(t)))
      } else {
        None
      }
    }
  }

  def resolveStar(sql: String)(implicit settings: Settings): String = {
    val schemaDefinition = settings.schemaHandler().objectJdbcDefinitions()
    val resolved =
      Try {
        val resolver = new JSQLColumResolver(schemaDefinition)
        resolver.setCommentFlag(false)
        resolver.getResolvedStatementText(sql)
      }
    resolved.getOrElse(sql)
  }

  def transpile(sql: String, conn: Connection, timestamps: Map[String, AnyRef]): String = {
    if (timestamps.nonEmpty) {
      logger.info(s"Transpiling SQL with timestamps: $timestamps")
    }
    val dialect = transpilerDialect(conn)
    val unpipedQuery = Try {
      if (dialect != JSQLTranspiler.Dialect.GOOGLE_BIG_QUERY) {
        JSQLTranspiler.unpipe(sql)
      } else {
        sql
      }
    } match {
      case Success(unpiped) =>
        unpiped
      case Failure(e) =>
        logger.error(s"Failed to unpipe SQL, sending as is to the dataware: $sql")
        Utils.logException(logger, e)
        sql
    }
    Try(
      JSQLTranspiler.transpileQuery(unpipedQuery, dialect, timestamps.asJava)
    ) match {
      case Success(transpiled) =>
        transpiled
      case Failure(e) =>
        logger.error(s"Failed to transpile SQL with dialect $dialect: $sql")
        Utils.logException(logger, e)
        sql
    }
  }

  def getSelectStatementIndex(sql: String): Int = {
    val trimmedSql = stripComments(sql.trim)

    // If the query doesn't start with WITH (case-insensitive and allowing whitespace), return 0
    val withPattern = "(?i)^\\s*WITH\\b".r
    if (withPattern.findFirstIn(trimmedSql).isEmpty) {
      0
    } else {

      var depth = 0
      var inSingleQuote = false
      var inDoubleQuote = false
      var i = 0
      var lastCteEnd = -1

      while (i < trimmedSql.length) {
        val c = trimmedSql(i)

        // Handle quotes - toggle quote state but only if not escaped
        if (c == '\'' && (i == 0 || trimmedSql(i - 1) != '\\')) {
          inSingleQuote = !inSingleQuote
        } else if (c == '"' && (i == 0 || trimmedSql(i - 1) != '\\')) {
          inDoubleQuote = !inDoubleQuote
        }

        // Only count parentheses outside of string literals
        if (!inSingleQuote && !inDoubleQuote) {
          c match {
            case '(' => depth += 1
            case ')' =>
              depth -= 1
              // If we've closed the last CTE definition
              if (depth == 0) {
                lastCteEnd = i
              }
            case _ =>
          }
        }
        i = i + 1
      }

      // If we found the end of the CTEs, look for the main SELECT
      if (lastCteEnd >= 0) {
        // Look for SELECT after the last CTE, considering case-insensitive matching
        val restOfSql = trimmedSql.substring(lastCteEnd + 1)
        val selectPattern = "(?i)\\bSELECT\\b".r
        selectPattern.findFirstMatchIn(restOfSql) match {
          case Some(m) => return lastCteEnd + 1 + m.start
          case None    => // No SELECT found, fall through to return -1
        }
      }

      // If we couldn't properly parse the query structure, return -1
      -1
    }
  }

  def addSelectItem(
    statement: String,
    columnName: String,
    columnExpr: Option[String] = None
  ): String = {
    val select = CCJSqlParserUtil.parse(statement).asInstanceOf[PlainSelect]
    columnExpr match {
      case Some(expr) =>
        val f = new Column(expr)
        val itemAdd = new SelectItem(f, new Alias(columnName, true))
        val items = select.getSelectItems
        select.getSelectItems.add(itemAdd)
      case None =>
        // If no expression is provided, we just add the column name
        val f = new Column(columnName)
        val itemAdd = new SelectItem(f)
        select.getSelectItems.add(itemAdd)
    }
    select.toString
  }
  def deleteSelectItem(statement: String, columnName: String): String = {
    val select = CCJSqlParserUtil.parse(statement).asInstanceOf[PlainSelect]
    val itemsToRemove = select.getSelectItems.asScala.filter { item =>
      item.getExpression() match {
        case col: Column => col.getColumnName == columnName
        case _ =>
          val alias = item.getAlias()
          alias != null && alias.getName == columnName
      }
    }
    itemsToRemove.foreach(select.getSelectItems.remove)
    select.toString
  }
  def upsertSelectItem(
    statement: String,
    columnName: String,
    columnExpr: Option[String] = None
  ): String = {
    val select = CCJSqlParserUtil.parse(statement).asInstanceOf[PlainSelect]
    val items = select.getSelectItems
    // update the column if it exists
    val indexToUpdate = items.asScala.indexWhere { item =>
      item.getExpression() match {
        case col: Column => col.getColumnName == columnName
        case _ =>
          val alias = item.getAlias()
          alias != null && alias.getName == columnName
      }
    }
    indexToUpdate match {
      case index if index >= 0 =>
        columnExpr match {
          case Some(expr) =>
            val f = new Column(expr)
            val updatedItem = new SelectItem(f, new Alias(columnName, true))
            items.set(index, updatedItem)
          case None =>
            // If no expression is provided, we just update the column name
            val f = new Column(columnName)
            val updatedItem = new SelectItem(f)
            items.set(index, updatedItem)
        }
      case _ => // -1
        // If the column does not exist, we add it
        addSelectItem(statement, columnName, columnExpr)
    }
    select.toString
  }

}
