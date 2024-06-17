package ai.starlake.job.sink.bigquery

import ai.starlake.utils.JobResult
import better.files.File
import com.google.cloud.bigquery._

import scala.jdk.CollectionConverters._

case class BigQueryJobResult(
  tableResult: scala.Option[TableResult],
  totalBytesProcessed: Long,
  job: scala.Option[Job]
) extends JobResult {

  def prettyPrint(format: String, rootServe: scala.Option[String]): String = {
    tableResult.map { rows =>
      val headers = rows.getSchema.getFields.iterator().asScala.toList.map(_.getName)
      val values =
        rows.iterateAll().asScala.toList.map { row =>
          row
            .iterator()
            .asScala
            .toList
            .map(cell => scala.Option(cell.getValue()).getOrElse("null").toString)
        }
      val result = prettyPrint(format, headers, values)
      result
    }
  }.getOrElse("")

  def show(format: String, rootServe: scala.Option[String]): Unit = {
    val output = rootServe.map(File(_, "extension.log"))
    output.foreach(_.append(s"Total Bytes Processed: $totalBytesProcessed bytes.\n"))
    println(s"Total Bytes Processed: $totalBytesProcessed bytes.")
    val res = prettyPrint(format, rootServe)
    println(res)
  }
}
