package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.config.Settings.Connection
import ai.starlake.lineage.ColLineage.Table
import ai.starlake.schema.handlers.{SchemaHandler, TableWithNameAndType}
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.{JdbcColumn, JdbcMetaData, JdbcResultSetMetaData}
import ai.starlake.utils.{JsonSerializer, ParseUtils, Utils}
import better.files.File
import com.typesafe.scalalogging.LazyLogging

import scala.jdk.CollectionConverters.*

class ColLineage(
  settings: Settings,
  schemaHandler: SchemaHandler
) extends LazyLogging {
  val domains = schemaHandler.domains()
  val tasks = schemaHandler.tasks()

  def getTableWithColumnNames(domain: String, table: String): Table = {
    domains
      .find(_.name.equalsIgnoreCase(domain))
      .flatMap { domain =>
        domain.tables.find(_.finalName.equalsIgnoreCase(table)).map { table =>
          Table(domain.name, table.name, table.attributes.map(_.getFinalName()), isTask = false)
        }
      }
      .getOrElse(Table(domain, table, Nil, isTask = false))
  }

  private def extractTables(column: JdbcColumn): List[Table] = {
    val thisTable = getTableWithColumnNames(column.tableSchema, column.tableName)

    val scopeTable = getTableWithColumnNames(column.scopeSchema, column.scopeTable)
    val childTables = column.getChildren.asScala.flatMap { child =>
      extractTables(child)
    }.toList
    val alTables = thisTable :: scopeTable :: childTables
    alTables.filter(it => it.table != null && it.table.nonEmpty)
  }
  private def extractTables(resultSetMetaData: JdbcResultSetMetaData): List[Table] = {
    val allTables = resultSetMetaData.getColumns.asScala
      .flatMap { column =>
        extractTables(column)
      }
      .toList
      .distinct
    val allTaskNames = schemaHandler.taskTableNames().map(_.toLowerCase)
    allTables.map { table =>
      val isTask = allTaskNames.contains(table.fullName.toLowerCase)
      table.copy(
        isTask = isTask,
        table = ColLineage.toLowerCase(table.table),
        domain = ColLineage.toLowerCase(table.domain)
      )
    }
  }

  def colLineage(config: ColLineageConfig): Option[ColLineage.Lineage] = {
    val taskDesc = schemaHandler.taskOnly(config.task, reload = true)
    val sql = config.sql.orElse(taskDesc.toOption.flatMap(_.sql))
    sql match {
      case Some(sql) =>
        taskDesc.toOption.flatMap { task =>
          sqlColLineage(
            config.outputFile,
            sql,
            task.name.split('.')(0),
            task.name.split('.')(1),
            task.getRunConnection()(settings)
          )
        }
      case None =>
        None
    }
  }

  def sqlColLineage(
    outputFile: Option[File],
    sql: String,
    domain: String,
    table: String,
    connection: Connection
  ): Option[ColLineage.Lineage] = {
    val sqlSubst = schemaHandler.substituteRefTaskMainSQL(sql, connection)
    if (sqlSubst.isEmpty) {
      logger.warn(s"Task $domain.$table has no SQL")
      None
    } else {
      val tableNames = SQLUtils.extractTableNames(sqlSubst)
      val quoteFreeTables = tableNames.map(SQLUtils.quoteFreeTableName)
      val tablesWithColumnNames = schemaHandler.getTablesWithColumnNames(quoteFreeTables)
      Some(
        colLineage(
          outputFile,
          domain,
          table,
          sqlSubst,
          tablesWithColumnNames
        )
      )
    }
  }

  def targetTableColumnNames(colLineage: ColLineage.Lineage) = {
    colLineage.relations.map(_.to.column)
  }

  def colLineage(
    outputFile: Option[File],
    domainName: String,
    tableName: String,
    sqlSubst: String,
    tablesWithColumnNames: List[(String, TableWithNameAndType)]
  ): ColLineage.Lineage = {
    val sql = sqlSubst.replaceAll("::[a-zA-Z0-9]+", "")
    // remove all ::TEXT (type change in columns)
    var jdbcMetadata = new JdbcMetaData("", "")
    tablesWithColumnNames.foreach { case (domainName, table) =>
      val jdbcColumns = table.attrs.map { case (attrName, attrType, comment) =>
        new JdbcColumn(attrName)
      }
      jdbcMetadata = jdbcMetadata.addTable("", domainName, table.name, jdbcColumns.asJava)
    }

    val res: JdbcResultSetMetaData =
      JSQLColumResolver
        .getResultSetMetaData(
          sql,
          JdbcMetaData.copyOf(jdbcMetadata.setErrorMode(JdbcMetaData.ErrorMode.LENIENT))
        )

    val tables = extractTables(res)
    val relations = ColLineage.extractRelations(domainName, tableName, res)
    val allTables = ColLineage.tablesInRelations(relations) ++ tables

    val finalTables = allTables
      .groupBy(t => (ColLineage.toLowerCase(t.domain), ColLineage.toLowerCase(t.table)))
      .map { case ((domainName, tableName), table) =>
        Table(
          domainName,
          tableName,
          table.flatMap(_.columns.map(_.toLowerCase())).distinct,
          isTask = table.exists(_.isTask)
        )
      }
      .toList
    logger.whenDebugEnabled {
      relations.foreach(r => logger.debug(r.toString))
      allTables.foreach(t => logger.debug(t.toString))
      finalTables.foreach(t => logger.debug(t.toString))
    }

    val lineage = ColLineage.Lineage(finalTables, relations)
    val diagramAsStr =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineage)
    Utils.save(outputFile, diagramAsStr)
    lineage
  }

}

object ColLineage {
  case class Column(domain: String, table: String, column: String)
  case class Relation(from: Column, to: Column, expression: Option[String])
  case class Table(domain: String, table: String, columns: List[String], isTask: Boolean) {
    def fullName: String = s"$domain.$table"
  }
  case class Lineage(tables: List[Table], relations: List[Relation]) {
    def diff(other: Lineage) = {
      // list all tables and columns present in one of the lineages and not in the other + all the common ones
      val inThis = this.tables.map(_.table).diff(other.tables.map(_.table))
      val inOther = other.tables.map(_.table).diff(this.tables.map(_.table))
      val inThisColumns = this.tables.map { table =>
        val otherTable = other.tables.find(_.table == table.table)
        otherTable match {
          case Some(otherTable) =>
            table.columns.diff(otherTable.columns)
          case None =>
            table.columns
        }
        (table.table, table.columns)
      }
      val inOtherColumns = other.tables.map { table =>
        val thisTable = this.tables.find(_.table == table.table)
        thisTable match {
          case Some(thisTable) =>
            table.columns.diff(thisTable.columns)
          case None =>
            table.columns
        }
        (table.table, table.columns)
      }
    }
  }

  def main(args: Array[String]) = {
    val query =
      """
        |create table orders ( customer_id int, order_id int, amount double, seller_id int);
        |create table customers ( id int, signup timestamp, contact string, birthdate date, name1 string, name2 string, id1 int);
        |with mycte as (
        |  select o.amount, c.id, CURRENT_TIMESTAMP() as timestamp1, o.amount as amount2
        |  from `orders` o, `customers` c
        |  where o.customer_id = c.id
        |),
        |yourcte as (select * from mycte)
        |select id, sumx(sumy(mycte.amount + yourcte.amount) + yourcte.amount) as sum, timestamp1, amount as amount2, yourcte.amount as amount3
        |from mycte, yourcte
        |group by yourcte.id, yourcte.timestamp1
        |""".stripMargin
    println(query)
    val (tables, select) = ParseUtils.parse(query, List("SELECT", "WITH", "FROM"))
    val lineage = ColLineage.lineageFromQuery(tables, select)
    val diagramAsStr =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineage)
    println(diagramAsStr)
  }
  def lineageFromQuery(
    inputTables: Array[(String, Array[(String, String)])],
    query: String
  ): Lineage = {
    val tables =
      inputTables.map { case (tableName, columns) =>
        Table("", tableName, columns.map(_._1).toList, isTask = false)

      }

    val metaData: JdbcMetaData =
      new JdbcMetaData("", "") // .setErrorMode(JdbcMetaData.ErrorMode.LENIENT)

    inputTables
      .foreach { case (tableName, columns) =>
        val jdbcColumns = columns.map { case (attrName, attrType) => new JdbcColumn(attrName) }
        metaData.addTable("", "", tableName, jdbcColumns.toList.asJava)
      }
    val res: JdbcResultSetMetaData =
      JSQLColumResolver
        .getResultSetMetaData(query, JdbcMetaData.copyOf(metaData))
        .asInstanceOf[JdbcResultSetMetaData]

    val relations =
      extractRelations("domain", "table", res)
    val allTables = ColLineage.tablesInRelations(relations) ++ tables.toList
    val finalTables = allTables
      .groupBy(t => (t.domain, t.table))
      .map { case ((domainName, tableName), table) =>
        Table(
          domainName,
          tableName,
          table.flatMap(_.columns).distinct,
          isTask = table.exists(_.isTask)
        )
      }
      .toList
    val lineage = ColLineage.Lineage(finalTables, relations)
    lineage
  }

  private def finalColumns(column: JdbcColumn): List[JdbcColumn] = {
    val children = column.getChildren.asScala.toList
    if (children.isEmpty) {
      if (
        Option(column.columnName).isDefined && column.columnName.nonEmpty &&
        Option(column.tableName).isDefined && column.tableName.nonEmpty
      ) {
        List(column)
      } else {
        Nil
      }
    } else {
      val finalCols = children.flatMap(finalColumns)
      finalCols
    }
  }

  def toLowerCase(str: String): String = {
    if (Option(str).isEmpty) {
      null
    } else {
      str.toLowerCase()
    }
  }

  def extractRelations(
    domainName: String,
    tableName: String,
    columnName: String,
    expression: Option[String],
    column: JdbcColumn
  ): List[Relation] = {
    val colName = Option(columnName).getOrElse(column.columnName)
    val currentColumn =
      Column(toLowerCase(domainName), toLowerCase(tableName), toLowerCase(colName))
    val relations =
      if (
        Option(column.tableName).isDefined &&
        column.tableName.nonEmpty &&
        Option(column.columnName).isDefined &&
        column.columnName.nonEmpty
      ) { // this is a not a function
        val parentColumn =
          Column(
            toLowerCase(column.tableSchema),
            toLowerCase(column.tableName),
            toLowerCase(column.columnName)
          )
        val immediateRelations =
          if (Option(column.scopeTable).isDefined && column.scopeTable.nonEmpty) {
            val scopeColumn = Column(
              toLowerCase(column.scopeSchema),
              toLowerCase(column.scopeTable),
              toLowerCase(column.columnName)
            )
            if (scopeColumn == parentColumn) {
              List(Relation(parentColumn, currentColumn, expression))
            } else {
              List(
                Relation(parentColumn, currentColumn, expression),
                Relation(scopeColumn, parentColumn, expression)
              )
            }
          } else if (expression.getOrElse("").contains("(")) {
            // This is a function, let's get all referenced table names
            val finalCols = finalColumns(column)
            val columnList =
              finalCols
                .filter(it => it.tableName != null && it.tableName.nonEmpty)
                .map { it =>
                  val col = Column(it.tableSchema, it.tableName, it.columnName)
                  Relation(col, parentColumn, expression)
                }
            columnList
          } else {
            List(Relation(parentColumn, currentColumn, expression))
          }
        val relations =
          column.getChildren.asScala.flatMap { child =>
            if (
              Option(child.tableName).isEmpty ||
              child.tableName.isEmpty ||
              Option(child.columnName).isEmpty ||
              child.columnName.isEmpty
            ) {
              Nil
            } else {
              extractRelations(
                column.tableSchema,
                column.tableName,
                column.columnName,
                Option(column.getExpression).map(_.toString),
                child
              )
            }
          }.toList

        immediateRelations ++ relations
      } else if (
        (Option(column.tableName).isEmpty || column.tableName.isEmpty) &&
        (Option(column.columnName).isDefined && column.columnName.nonEmpty)
      ) { // this is a function
        val relations =
          column.getChildren.asScala.flatMap { child =>
            extractRelations(
              domainName,
              tableName,
              colName,
              Option(column.getExpression).map(_.toString),
              child
            )
          }.toList
        relations
      } else {
        Nil
      }
    relations
  }

  def extractRelations(
    domainName: String,
    tableName: String,
    resultSetMetaData: JdbcResultSetMetaData
  ): List[Relation] = {
    val columns = resultSetMetaData.getColumns.asScala
    val labels = resultSetMetaData.getLabels.asScala
    assert(columns.size == labels.size)
    val relations =
      columns
        .zip(labels)
        .flatMap { case (column, label) =>
          val columnName = if (Option(label).isEmpty) column.columnName else label
          ColLineage.extractRelations(
            domainName,
            tableName,
            columnName,
            Option(column.getExpression).map(_.toString),
            column
          )
        }
        .toList
    relations.distinct
  }
  def tablesInRelations(relations: List[Relation]): List[Table] = {
    val tables =
      relations.flatMap { relation =>
        val table1 =
          Table(
            ColLineage.toLowerCase(relation.from.domain),
            ColLineage.toLowerCase(relation.from.table),
            List(ColLineage.toLowerCase(relation.from.column)),
            isTask = false
          )
        val table2 =
          Table(
            ColLineage.toLowerCase(relation.to.domain),
            ColLineage.toLowerCase(relation.to.table),
            List(ColLineage.toLowerCase(relation.to.column)),
            isTask = false
          )
        List(table1, table2)
      }
    tables
  }
}
