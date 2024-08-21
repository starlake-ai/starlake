package ai.starlake.sql

import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.{JdbcColumn, JdbcMetaData, JdbcResultSetMetaData}

import scala.jdk.CollectionConverters._

class Lineage {
  def run1() {
    val query =
      """
      |with mycte as (
      |  select o.amount, c.id, CURRENT_TIMESTAMP() as timestamp1, o.amount as amount2
      |  from `sales`.`orders` o, `sales.customers` c
      |  where o.customer_id = c.id
      |),
      |yourcte as (select o.amount from sales.orders o)
      |select id, sumx(sumy(mycte.amount + yourcte.amount) + yourcte.amount) as sum, timestamp1, amount as amount2, yourcte.amount as amount3
      |from mycte, yourcte
      |group by yourcte.id, yourcte.timestamp1
      |""".stripMargin
    println(query)
    val metaData: JdbcMetaData = new JdbcMetaData("", "")
      .addTable(
        "sales",
        "orders",
        new JdbcColumn("customer_id"),
        new JdbcColumn("order_id"),
        new JdbcColumn("amount"),
        new JdbcColumn("seller_id")
      )
      .addTable(
        "sales",
        "customers",
        new JdbcColumn("id"),
        new JdbcColumn("signup"),
        new JdbcColumn("contact"),
        new JdbcColumn("birthdate"),
        new JdbcColumn("name1"),
        new JdbcColumn("name2"),
        new JdbcColumn("id1")
      )

    val res: JdbcResultSetMetaData =
      JSQLColumResolver
        .getResultSetMetaData(query, JdbcMetaData.copyOf(metaData))
        .asInstanceOf[JdbcResultSetMetaData]

    Lineage.extractTables(res).foreach(println)
    Lineage.extractRelations("mySchema", "myTable", res).foreach(println)
  }
}

object Lineage {

  case class Column(schema: String, table: String, column: String)
  case class Relation(from: Column, to: Column, expression: Option[String])
  case class Table(schema: String, table: String)

  def extractTables(column: JdbcColumn): List[Table] = {
    val thisTable = Table(column.tableSchema, Option(column.tableName).getOrElse(""))
    val scopeTable = Table(column.scopeSchema, Option(column.scopeTable).getOrElse(""))
    val childTables = column.getChildren.asScala.flatMap { child =>
      extractTables(child)
    }.toList
    val alTables = thisTable :: scopeTable :: childTables
    alTables.filter(_.table.nonEmpty)
  }
  def extractTables(resultSetMetaData: JdbcResultSetMetaData): List[Table] = {
    resultSetMetaData.getColumns.asScala.flatMap { column =>
      extractTables(column)
    }.toList
  }

  def extractRelations(
    schemaName: String,
    tableName: String,
    columnName: String,
    expression: Option[String],
    column: JdbcColumn
  ): List[Relation] = {
    val colName = Option(columnName).getOrElse(column.columnName)
    val currentColumn = Column(schemaName, tableName, colName)
    val relations =
      if (column.tableName.nonEmpty && column.columnName.nonEmpty) { // this is a not a function
        val parentColumn = Column(column.tableSchema, column.tableName, column.columnName)
        val immediateRelations =
          if (column.scopeTable.nonEmpty) {
            val scopeColumn = Column(column.scopeSchema, column.scopeTable, column.columnName)
            List(
              Relation(parentColumn, currentColumn, expression),
              Relation(scopeColumn, parentColumn, expression)
            )
          } else {
            List(Relation(parentColumn, currentColumn, expression))
          }
        assert(column.getChildren.size() == 0)
        immediateRelations
      } else if (column.tableName.isEmpty && column.columnName.nonEmpty) { // this is a function
        val relations =
          column.getChildren.asScala.flatMap { child =>
            extractRelations(
              schemaName,
              tableName,
              colName,
              Option(column.getExpression).map(_.toString),
              child
            )
          }.toList
        relations
      } else {
        assert(false)
        Nil
      }
    relations
  }
  def extractRelations(
    schemaName: String,
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
          extractRelations(
            schemaName,
            tableName,
            columnName,
            Option(column.getExpression).map(_.toString),
            column
          )
        }
        .toList
    relations.distinct
  }

  def main(args: Array[String]): Unit = {
    val ln = new Lineage()
    ln.run1()
  }
}
