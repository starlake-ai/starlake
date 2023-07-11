package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.schema.model.{AutoJobDesc, Domain, Engine, OutputRef, Refs}
import com.typesafe.scalalogging.StrictLogging
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SelectVisitorAdapter}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object SQLUtils extends StrictLogging {
  val fromsRegex = "(?i)\\s+FROM\\s+([_\\-a-z0-9`./(]+\\s*[ _,a-z0-9`./(]*)".r
  val joinRegex = "(?i)\\s+JOIN\\s+([_\\-a-z0-9`./]+)".r
  val cteRegex = "(?i)\\s+([a-z0-9]+)+\\s+AS\\s*\\(".r

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
  def extractRefsInSQL(sql: String): List[String] = {
    val froms =
      fromsRegex
        .findAllMatchIn(sql)
        .map(_.group(1))
        .toList
        .flatMap(_.split(",").map(_.trim))
        .map {
          // because the regex above is not powerful enough
          table =>
            val space = table.replaceAll("\n", " ").replace("\t", " ").indexOf(' ')
            if (space > 0)
              table.substring(0, space)
            else
              table
        }
        .filter(!_.contains("(")) // we remove constructions like 'year from date(...)'

    val joins = joinRegex.findAllMatchIn(sql).map(_.group(1)).toList
    // val ctes = cteRegex.findAllMatchIn(sql).map(_.group(1)).toList

    (froms ++ joins).map(_.replaceAll("`", ""))
  }

  def extractCTEsFromSQL(sql: String): List[String] = {
    val ctes = cteRegex.findAllMatchIn(sql).map(_.group(1)).toList
    ctes.map(_.replaceAll("`", ""))
  }

  def extractColumnNames(sql: String): List[String] = {
    var result: List[String] = Nil
    val selectVisitorAdapter = new SelectVisitorAdapter() {
      override def visit(plainSelect: PlainSelect): Unit = {
        result = plainSelect.getSelectItems.asScala.map { selectItem =>
          selectItem.getASTNode.jjtGetLastToken().image
        }.toList
      }
    }
    val statementVisitor = new StatementVisitorAdapter() {
      override def visit(select: Select): Unit = {
        select.getSelectBody.accept(selectVisitorAdapter)
      }
    }
    val select = CCJSqlParserUtil.parse(sql)
    select.accept(statementVisitor)
    result
  }

  def extractCTENames(sql: String): List[String] = {
    var result: ListBuffer[String] = ListBuffer()
    val statementVisitor = new StatementVisitorAdapter() {
      override def visit(select: Select): Unit = {
        select.getWithItemsList().asScala.foreach { withItem =>
          result :+ withItem.getName()
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
    engine: Engine
  ): String = {

    key match {
      case Nil => sql
      case cols =>
        val joinCondition = cols
          .map { col =>
            s"SL_INTERNAL_SOURCE.$col = SL_INTERNAL_SINK.$col"
          }
          .mkString(" AND ")

        val fullTableName = database match {
          case Some(db) => OutputRef(db, domain, table).toSQLString(engine)
          case None     => OutputRef("", domain, table).toSQLString(engine)
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
    j2sql: String,
    vars: Map[String, Any],
    refs: Refs,
    domains: List[Domain],
    jobs: Map[String, AutoJobDesc],
    localViews: List[String],
    engine: Engine
  )(implicit
    settings: Settings
  ): String = {
    logger.info(s"Source J2SQL: $j2sql")
    val sql = Utils.parseJinja(j2sql, vars)
    val fromResolved =
      buildSingleSQLQueryForRegex(
        sql,
        vars,
        refs,
        domains,
        jobs,
        localViews,
        SQLUtils.fromsRegex,
        "FROM",
        engine
      )
    val joinAndFromResolved =
      buildSingleSQLQueryForRegex(
        fromResolved,
        vars,
        refs,
        domains,
        jobs,
        localViews,
        SQLUtils.joinRegex,
        "JOIN",
        engine
      )
    joinAndFromResolved
  }

  def buildSingleSQLQueryForRegex(
    sql: String,
    vars: Map[String, Any],
    refs: Refs,
    domains: List[Domain],
    jobs: Map[String, AutoJobDesc],
    localViews: List[String],
    regex: Regex,
    keyword: String,
    engine: Engine
  )(implicit
    settings: Settings
  ): String = {
    logger.info(s"Source SQL: $sql")
    val ctes = SQLUtils.extractCTEsFromSQL(sql) ++ localViews
    var resolvedSQL = ""
    var startIndex = 0
    val fromMatches = regex
      .findAllMatchIn(sql)
      .toList
    if (fromMatches.isEmpty) {
      sql
    } else {
      def ltrim(s: String) = s.replaceAll("^\\s+", "")
      fromMatches
        .foreach { regex =>
          val source = ltrim(regex.source.toString.substring(regex.start, regex.end))
          val tablesAndAlias = source.substring(keyword.length).split(",")
          val tableAndAliasFinalNames = tablesAndAlias.map { tableAndAlias =>
            resolveTableNameInSql(tableAndAlias, refs, domains, jobs, ctes, engine)
          }
          val newSource = tableAndAliasFinalNames.mkString(", ")
          val newFrom = s" $keyword $newSource"
          resolvedSQL += sql.substring(startIndex, regex.start) + newFrom
          startIndex = regex.end
        }
      resolvedSQL = resolvedSQL + sql.substring(startIndex)
      logger.info(s"Resolved SQL: $resolvedSQL")
      resolvedSQL
    }
  }

  private def resolveTableNameInSql(
    tableAndAlias: String,
    refs: Refs,
    domains: List[Domain],
    jobs: Map[String, AutoJobDesc],
    ctes: List[String],
    engine: Engine
  )(implicit
    settings: Settings
  ): String = {
    def cteContains(table: String): Boolean = ctes.exists(cte => cte.equalsIgnoreCase(table))

    val tableAndAliasArray = tableAndAlias.trim.split("\\s")
    val tableName = tableAndAliasArray.head
    val tableTuple = tableName.replaceAll("`", "").split("\\.").toList
    if (cteContains(tableName) || tableName.contains("/") || tableName.contains("(")) {
      // this is a parquet reference with Spark syntax
      // or a function: from date(...)
      // or a CTE reference
      tableAndAlias
    } else {
      val activeEnvRefs = refs
      val databaseDomainTableRef =
        activeEnvRefs.getOutputRef(tableTuple).map(_.toSQLString(engine))
      val resolvedTableName = databaseDomainTableRef.getOrElse {
        resolveTableRefInDomainsAndJobs(tableTuple, domains, jobs) match {
          case Success((database, domain, table)) =>
            ai.starlake.schema.model.OutputRef(database, domain, table).toSQLString(engine)
          case Failure(e) =>
            Utils.logException(logger, e)
            throw e
        }
      }
      resolvedTableName + " " + tableAndAliasArray.tail.mkString(" ")
    }
  }

  private def resolveTableRefInDomainsAndJobs(
    tableComponents: List[String],
    domains: List[Domain],
    jobs: Map[String, AutoJobDesc]
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
        val tasksByTable = jobs.values.flatMap { job =>
          job.tasks.find { task =>
            val domainOK = domainComponent.forall(_.equalsIgnoreCase(task.domain))
            domainOK && task.table.equalsIgnoreCase(table)
          }
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
          (None, "")
        }
        val databaseName = database
          .orElse(settings.comet.getDatabase())
          .getOrElse("")
        (databaseName, domain, table)
      case _ =>
        throw new Exception(
          s"Invalid table reference ${tableComponents.mkString(".")}"
        )
    }

  }

}
