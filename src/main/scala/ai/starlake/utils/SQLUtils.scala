package ai.starlake.utils

import ai.starlake.schema.model.Engine
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SelectVisitorAdapter}

import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.matching.Regex

object SQLUtils {
  val fromsRegex = "(?i)\\s+FROM\\s+([_\\-a-z0-9`./(]+\\s*[ _,a-z0-9`./(]*)".r
  val joinRegex = "(?i)\\s+JOIN\\s+([_\\-a-z0-9`./]+)".r
  // val cteRegex = "(?i)\\s+([a-z0-9]+)+\\s+AS\\s*\\(".r

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
  def extractRefsFromSQL(sql: String): List[String] = {
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
    val cteRegex = "(?i)\\s+([a-z0-9]+)+\\s+AS\\s*\\(".r
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

    val quotes = Map(
      Engine.SPARK.toString -> "`",
      "SNOWFLAKE"           -> "\"",
      Engine.BQ.toString    -> "`"
    )
    val quote = quotes(engine.toString)
    key match {
      case Nil => sql
      case cols =>
        val joinCondition = cols
          .map { col =>
            s"SL_INTERNAL_SOURCE.$col = SL_INTERNAL_SINK.$col"
          }
          .mkString(" AND ")

        val fullTableName = database match {
          case Some(db) => s"$quote$db$quote.$quote$domain$quote.$quote$table$quote"
          case None     => s"$quote$domain$quote.$quote$table$quote"
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

  def resolveRefsInSQL(
    sql: String,
    refMap: List[Option[(String, String, String)]] // (database, domain, table)
  ): String = {
    def getPrefix(sql: String, start: Int): String = {
      val substr = sql.substring(start).trim
      val withFrom = substr.toUpperCase.startsWith("FROM")
      val withJoin = substr.toUpperCase.trim.startsWith("JOIN")
      if (withFrom)
        " FROM "
      else if (withJoin)
        " JOIN "
      else
        ""
    }

    val iterator = refMap.iterator
    var result = sql
    def replaceRegexMatches(matches: Iterator[Regex.Match]): Unit = {
      matches.toList.reverse
        .foreach { regex =>
          iterator.next() match {
            case Some((_, "", table)) =>
            // do nothing

            case Some(("", domain, table)) =>
              val prefix: String = getPrefix(result, regex.start)
              result = result.substring(0, regex.start) +
                s"$prefix$domain.$table" +
                result.substring(regex.end)

            case Some((database, domain, table)) =>
              val prefix: String = getPrefix(result, regex.start)
              result = result.substring(0, regex.start) +
                s"$prefix$database.$domain.$table" +
                result.substring(regex.end)
            case None =>
          }
        }
    }

    val fromMatches = fromsRegex
      .findAllMatchIn(sql)
    replaceRegexMatches(fromMatches)
    val joinMatches: Iterator[Regex.Match] = joinRegex
      .findAllMatchIn(result)
    replaceRegexMatches(joinMatches)
    result
  }
}
