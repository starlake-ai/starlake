package ai.starlake.utils

object TableFormatter {
  // https://stackoverflow.com/questions/7539831/scala-draw-table-to-console

  def format(table: Seq[Seq[String]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes =
        for (row <- table)
          yield for (cell <- row) yield if (cell == null) 0 else cell.length
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  private def rowSeparator(colSizes: Seq[Int]): String = colSizes map {
    "-" * _
  } mkString ("+", "+", "+")

  private def formatRows(rowSeparator: String, rows: Seq[String]): String =
    (
      rowSeparator ::
        rows.head ::
        rowSeparator ::
        rows.tail.toList :::
        rowSeparator ::
        Nil
    ).mkString("\n")

  private def formatRow(row: Seq[Any], colSizes: Seq[Int]) = {
    val cells =
      for ((item, size) <- row.zip(colSizes))
        yield if (size == 0) "" else ("%" + size + "s").format(item)
    cells.mkString("|", "|", "|")
  }
}
