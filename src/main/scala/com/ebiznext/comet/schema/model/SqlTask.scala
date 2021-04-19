package com.ebiznext.comet.schema.model

import better.files.File
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ListBuffer

case class SqlTask(presql: Option[List[String]], sql: String, postsql: Option[List[String]])

object SqlTask {

  def apply(path: Path): SqlTask = {
    val cometPattern = "^\\s*/\\*\\s*(SQL|PRESQL|POSTSQL)\\s*\\*/\\s*$".r
    val sqlFile = File(path.toString)
    val buffer = new StringBuffer()
    val presqlSection = ListBuffer.empty[String]
    val postsqlSection = ListBuffer.empty[String]
    val sqlSection = new StringBuffer()

    def appendToStep(buffer: StringBuffer, section: String): Unit = {
      val sql = buffer.toString.trim
      if (sql.nonEmpty) {
        section match {
          case "SQL" =>
            if (sql.size > 0 && sqlSection.toString.size == 0)
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
    sqlFile.lines.foreach {
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
    SqlTask(
      if (presqlSection.isEmpty) None else Some(presqlSection.toList),
      sqlSection.toString,
      if (postsqlSection.isEmpty) None else Some(postsqlSection.toList)
    )
  }
}
