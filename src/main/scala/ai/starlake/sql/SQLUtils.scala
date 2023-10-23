package ai.starlake.sql

import ai.starlake.config.Settings
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SelectVisitorAdapter}
import net.sf.jsqlparser.util.TablesNamesFinder

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
    val selectVisitorAdapter = new SelectVisitorAdapter() {
      override def visit(plainSelect: PlainSelect): Unit = {
        val selectItems = Option(plainSelect.getSelectItems).map(_.asScala).getOrElse(Nil)
        result = selectItems.map { selectItem =>
          selectItem.getASTNode.jjtGetLastToken().image
        }.toList
      }
    }
    val statementVisitor = new StatementVisitorAdapter() {
      override def visit(select: Select): Unit = {
        select.accept(selectVisitorAdapter)
      }
    }
    val select = CCJSqlParserUtil.parse(sql)
    select.accept(statementVisitor)
    result
  }

  def extractTableNames(sql: String): List[String] = {
    val select = CCJSqlParserUtil.parse(sql)
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
    val select = CCJSqlParserUtil.parse(sql)
    select.accept(statementVisitor)
    result.toList
  }

  private def buildWhenNotMatchedInsert(sql: String): String = {
    val columnNames = SQLUtils.extractColumnNames(sql)
    val columnNamesString = columnNames.map(colName => s""""$colName"""").mkString("(", ",", ")")
    val columnValuesString = columnNames.mkString("(", ",", ")")
    s"""WHEN NOT MATCHED THEN INSERT $columnNamesString VALUES $columnValuesString"""
  }

  private def buildWhenMatchedUpdate(sql: String) = {
    val columnNames = SQLUtils.extractColumnNames(sql)
    columnNames
      .map { colName =>
        s"""$colName = SL_INTERNAL_SOURCE.$colName"""
      }
      .mkString("WHEN MATCHED THEN UPDATE SET ", ", ", "")
  }

  def buildMergeSql(
    sql: String,
    key: List[String],
    database: Option[String],
    domain: String,
    table: String,
    engine: Engine,
    isFilesystem: Boolean
  )(implicit settings: Settings): String = {

    key match {
      case Nil => sql
      case cols =>
        val joinCondition = cols
          .map { col =>
            s"SL_INTERNAL_SOURCE.$col = SL_INTERNAL_SINK.$col"
          }
          .mkString(" AND ")

        val fullTableName = database match {
          case Some(db) =>
            OutputRef(db, domain, table).toSQLString(engine, isFilesystem)
          case None =>
            OutputRef("", domain, table).toSQLString(engine, isFilesystem)
        }
        s"""MERGE INTO
           |$fullTableName as SL_INTERNAL_SINK
           |USING($sql) as SL_INTERNAL_SOURCE ON $joinCondition
           |${buildWhenMatchedUpdate(sql)}
           |
           |${buildWhenNotMatchedInsert(sql)}
           |
           |""".stripMargin

    }
  }

  def buildSingleSQLQuery(
    sql: String,
    refs: Refs,
    domains: List[Domain],
    tasks: List[AutoTaskDesc],
    localViews: List[String],
    engine: Engine
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
        localViews,
        SQLUtils.fromsRegex,
        "FROM",
        engine
      )
    val joinAndFromResolved =
      buildSingleSQLQueryForRegex(
        fromResolved,
        refs,
        domains,
        tasks,
        localViews,
        SQLUtils.joinRegex,
        "JOIN",
        engine
      )
    joinAndFromResolved
  }

  def buildSingleSQLQueryForRegex(
    sql: String,
    refs: Refs,
    domains: List[Domain],
    tasks: List[AutoTaskDesc],
    localViews: List[String],
    fromOrJoinRegex: Regex,
    keyword: String,
    engine: Engine
  )(implicit
    settings: Settings
  ): String = {
    logger.info(s"Source SQL: $sql")
    val ctes = SQLUtils.extractCTENames(sql) ++ localViews
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
              engine,
              localViews.nonEmpty
            )
            source = source.replaceAll(tableFound, resolvedTableName)
          }
          resolvedSQL += sql.substring(startIndex, regex.start) + source
          startIndex = regex.end
        }
      resolvedSQL = resolvedSQL + sql.substring(startIndex)
      logger.info(s"Resolved SQL: $resolvedSQL")
      resolvedSQL
    }
  }

  private def resolveTableNameInSql(
    tableName: String,
    refs: Refs,
    domains: List[Domain],
    tasks: List[AutoTaskDesc],
    ctes: List[String],
    engine: Engine,
    isFilesystem: Boolean
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
      if (isFilesystem) {
        // We keep only the table name, the database and domain names are ignored for filesystem
        tableTuple.last
      } else {
        // We need to find it in the refs
        val activeEnvRefs = refs
        val databaseDomainTableRef =
          activeEnvRefs
            .getOutputRef(tableTuple)
            .map(_.toSQLString(engine, isFilesystem))
        val resolvedTableName = databaseDomainTableRef.getOrElse {
          resolveTableRefInDomainsAndJobs(tableTuple, domains, tasks) match {
            case Success((database, domain, table)) =>
              ai.starlake.schema.model
                .OutputRef(database, domain, table)
                .toSQLString(engine, isFilesystem)
            case Failure(e) =>
              Utils.logException(logger, e)
              throw e
          }
        }
        resolvedTableName
      }
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

}
