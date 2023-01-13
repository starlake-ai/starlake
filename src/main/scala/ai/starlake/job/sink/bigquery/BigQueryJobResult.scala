package ai.starlake.job.sink.bigquery

import ai.starlake.utils.{JobResult, TableFormatter}
import better.files.File
import com.google.cloud.bigquery._
import com.google.gson.Gson

import scala.collection.JavaConverters._

case class BigQueryJobResult(tableResult: scala.Option[TableResult], totalBytesProcessed: Long)
    extends JobResult {

  def show(format: String, rootServe: scala.Option[String]): Unit = {
    val output = rootServe.map(File(_, "run.log"))
    output.foreach(_.overwrite(s"Total Bytes Processed: $totalBytesProcessed bytes.\n"))
    println(s"Total Bytes Processed: $totalBytesProcessed bytes.")
    tableResult.foreach { rows =>
      val headers = rows.getSchema.getFields.iterator().asScala.toList.map(_.getName)
      val values =
        rows.getValues.iterator().asScala.toList.map { row =>
          row
            .iterator()
            .asScala
            .toList
            .map(cell => scala.Option(cell.getValue()).getOrElse("null").toString)
        }

      format match {
        case "csv" =>
          (headers :: values).foreach { row =>
            output.foreach(_.appendLine(row.mkString(",")))
            println(row.mkString(","))
          }

        case "table" =>
          headers :: values match {
            case Nil =>
              output.foreach(_.appendLine("Result is empty."))
              println("Result is empty.")
            case _ =>
              output.foreach(_.appendLine(TableFormatter.format(headers :: values)))
              println(TableFormatter.format(headers :: values))
          }

        case "json" =>
          values.foreach { value =>
            val map = headers.zip(value).toMap
            val json = new Gson().toJson(map.asJava)
            output.foreach(_.appendLine(json))
            println(json)
          }
      }
    }
  }
}
