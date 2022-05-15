package ai.starlake.schema.model

import scala.collection.mutable.ListBuffer

case class SqlTaskExtractor(
  presql: Option[List[String]],
  sql: String,
  postsql: Option[List[String]]
)

object SqlTaskExtractor {

  def apply(sqlContent: String): SqlTaskExtractor = {
    val cometPattern = "^\\s*/\\*\\s*(SQL|PRESQL|POSTSQL)\\s*\\*/\\s*$".r
    val sqlFileLines = sqlContent.split("\n")
    val buffer = new StringBuffer
    val presqlSection = ListBuffer.empty[String]
    val postsqlSection = ListBuffer.empty[String]
    val sqlSection = new StringBuffer

    def appendToStep(buffer: StringBuffer, section: String): Unit = {
      val sql = buffer.toString.trim
      if (sql.nonEmpty) {
        section match {
          case "SQL" =>
            if (sql.nonEmpty && sqlSection.toString.isEmpty)
              sqlSection.append(sql)
          case "PRESQL" =>
            presqlSection.append(sql)
          case "POSTSQL" =>
            postsqlSection.append(sql)
        }
        buffer.delete(0, buffer.length())
      }
    }

    var section = "SQL"
    sqlFileLines.foreach {
      case cometPattern("SQL") =>
        appendToStep(buffer, section)
        section = "SQL"
      case cometPattern("PRESQL") =>
        appendToStep(buffer, section)
        section = "PRESQL"
      case cometPattern("POSTSQL") =>
        appendToStep(buffer, section)
        section = "POSTSQL"
      case line =>
        val trimmed = line.trim
        if (trimmed.nonEmpty)
          buffer.append(trimmed).append('\n')
    }
    appendToStep(buffer, section)
    SqlTaskExtractor(
      if (presqlSection.isEmpty) None else Some(presqlSection.toList),
      sqlSection.toString,
      if (postsqlSection.isEmpty) None else Some(postsqlSection.toList)
    )
  }
}
