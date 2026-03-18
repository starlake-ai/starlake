package ai.starlake.sql

import com.typesafe.scalalogging.LazyLogging
import net.sf.jsqlparser.parser.{CCJSqlParser, CCJSqlParserUtil}
import net.sf.jsqlparser.statement.select.{PlainSelect, SelectVisitorAdapter, SetOperationList, Select}
import net.sf.jsqlparser.statement.{Statement, StatementVisitorAdapter}
import net.sf.jsqlparser.util.TablesNamesFinder

import java.util.function.Consumer
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object SqlParser extends LazyLogging {

  val fromsRegex = "(?i)\\s+FROM\\s+([_\\-a-z0-9`./(]+\\s*[ _,a-z0-9`./(]*)".r
  val joinRegex = "(?i)\\s+JOIN\\s+([_\\-a-z0-9`./]+)".r

  def extractTableNamesUsingRegEx(sql: String): List[String] = {
    val froms =
      fromsRegex
        .findAllMatchIn(sql)
        .map(_.group(1))
        .toList
        .flatMap(_.split(",").map(_.trim))
        .map { table =>
          val space = table.replaceAll("\\s", " ").indexOf(' ')
          if (space > 0) table.substring(0, space) else table
        }
        .filter(!_.contains("("))

    val joins = joinRegex.findAllMatchIn(sql).map(_.group(1)).toList
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
    val unquoted = tableList.map { domainAndTableName =>
      SqlFormatter.unquoteAgressive(domainAndTableName.split("\\.").toList).mkString(".")
    }
    unquoted.toList.distinct
  }

  def extractCTENames(sql: String): List[String] = {
    var result: ListBuffer[String] = ListBuffer()
    val statementVisitor = new StatementVisitorAdapter[Any]() {
      override def visit[T](select: Select, context: T): Any = {
        val ctes = Option(select.getWithItemsList()).map(_.asScala).getOrElse(Nil)
        ctes.foreach { withItem =>
          val alias = Option(withItem.getAlias).map(_.getName).getOrElse("")
          if (alias.nonEmpty)
            result += SqlFormatter.unquoteAgressive(alias)
        }
        null
      }
    }
    val select = jsqlParse(sql)
    select.accept(statementVisitor, null)
    result.toList
  }

  def jsqlParse(sql: String): Statement = {
    val features = new Consumer[CCJSqlParser] {
      override def accept(t: CCJSqlParser): Unit = {
        t.withTimeOut(60 * 1000)
      }
    }
    try {
      CCJSqlParserUtil.parse(sql.trim, features)
    } catch {
      case exception: Exception =>
        logger.error(s"Failed to parse $sql")
        throw exception
    }
  }

  def getSelectStatementIndex(sql: String): Int = {
    val trimmedSql = SqlFormatter.stripComments(sql.trim)

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
        if (c == '\'' && (i == 0 || trimmedSql(i - 1) != '\\')) {
          inSingleQuote = !inSingleQuote
        } else if (c == '"' && (i == 0 || trimmedSql(i - 1) != '\\')) {
          inDoubleQuote = !inDoubleQuote
        }
        if (!inSingleQuote && !inDoubleQuote) {
          c match {
            case '(' => depth += 1
            case ')' =>
              depth -= 1
              if (depth == 0) { lastCteEnd = i }
            case _ =>
          }
        }
        i = i + 1
      }

      if (lastCteEnd >= 0) {
        val restOfSql = trimmedSql.substring(lastCteEnd + 1)
        val selectPattern = "(?i)\\bSELECT\\b".r
        selectPattern.findFirstMatchIn(restOfSql) match {
          case Some(m) => return lastCteEnd + 1 + m.start
          case None    =>
        }
      }
      -1
    }
  }
}