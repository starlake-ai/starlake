package org.apache.spark.sql.catalyst.parser

object ParserSQL {
  /*
  import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
  case class Deps(tables: List[String], aliases: Map[String, List[String]]) {
    def addTable(name: String): Deps = copy(tables = name :: tables)

    def toAlias(name: String): Deps = Deps.zero.copy(aliases = Map(name.toUpperCase -> this.tables))

    def ++(deps: Deps): Deps = {
      def dealias(name: String): List[String] = aliases.getOrElse(name.toUpperCase, List(name))

      (this, deps) match {
        case (Deps.zero, _) => deps
        case (_, Deps.zero) => this
        case (_, _) =>
          Deps(
            tables ++ deps.tables.flatMap(dealias),
            aliases ++ deps.aliases.mapValues(_.flatMap(dealias))
          )
      }
    }
  }

  object Deps {
    val zero: Deps = Deps(Nil, Map.empty)
  }

  def getTablesName(sql: String): List[String] = {

    val depsBuilder = new SqlBaseParserBaseVisitor[Deps] {
      override def visitTableName(ctx: SqlBaseParser.TableNameContext): Deps =
        super.visitTableName(ctx).addTable(ctx.identifierReference().getText())

      override def visitNamedQuery(ctx: SqlBaseParser.NamedQueryContext): Deps =
        super.visitNamedQuery(ctx).toAlias(ctx.name.getText)

      override def defaultResult(): Deps = Deps.zero
      override def aggregateResult(aggregate: Deps, nextResult: Deps): Deps =
        aggregate ++ nextResult
    }

    def parse(sql: String): List[String] = {
      val lexer = new SqlBaseLexer(
        new UpperCaseCharStream(CharStreams.fromString(sql))
      )
      val tokenStream = new CommonTokenStream(lexer)
      val parser = new SqlBaseParser(tokenStream)
      val parsed = parser.singleStatement()
      val res = depsBuilder.visitSingleStatement(parsed)
      res.tables.distinct
    }

    val sqlWithoutSpecificBQ = sql
      .replaceAll("(?i)except[\\s(]+[^)]*\\)", "")
      .replaceAll("(?i)extract[\\s(]+.*?from.*?[\\s)]*\\)", "dateExtract")
      .replaceAll("(?i)safe_cast[\\s(]+.*?as.*?[\\s)]*\\)", "castField")
      .replaceAll("(?i)\\[[^]]*\\]", "list")
      .replaceAll("(?i)unnest", "")

    val tables = if (sqlWithoutSpecificBQ.nonEmpty) parse(sqlWithoutSpecificBQ) else Nil
    tables.map(_.replaceAll("`", "")) // bigquery project.dataset.table no legacy sql
  }
   */
}
