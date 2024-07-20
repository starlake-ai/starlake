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

  private def flatten(fieldList: List[Field], level: Int = 0): List[Map[String, String]] = {
    fieldList.flatMap { field =>
      val space = " " * 4 * level
      val hasSubFields = Option(field.getSubFields).isDefined && !field.getSubFields.isEmpty
      val fieldName = space + field.getName
      val fieldMap =
        Map(
          "Field name"    -> fieldName,
          "Type"          -> field.getType.toString,
          "Mode"          -> field.getMode.toString,
          "Default Value" -> Option(field.getDefaultValueExpression).getOrElse(""),
          "Policy Tags" -> Option(field.getPolicyTags)
            .map(_.getNames.asScala.mkString(","))
            .getOrElse(""),
          "Description" -> Option(field.getDescription).getOrElse("")
        )
      if (!hasSubFields) {
        List(fieldMap)
      } else {
        List(fieldMap) ++ flatten(field.getSubFields.asScala.toList, level + 1)
      }
    }
  }

  override def asMap(): List[Map[String, String]] = {
    if (this.totalBytesProcessed < 0) {
      // The result is the schema of the table
      tableResult
        .map { tableResult =>
          val fieldList = tableResult.getSchema.getFields.iterator().asScala.toList
          flatten(fieldList, 0)
        }
        .getOrElse(Nil)
    } else {
      tableResult
        .map { rows =>
          val headers = rows.getSchema.getFields.iterator().asScala.toList.map(_.getName)
          val result =
            rows.iterateAll().asScala.toList.map { row =>
              val values = row
                .iterator()
                .asScala
                .toList
                .map { cell =>
                  scala.Option(cell.getValue()).map(_.toString).getOrElse("NULL")
                }
              headers.zip(values).toMap
            }
          result
        }
        .getOrElse(Nil)
    }
  }

  override def prettyPrint(format: String): String = {
    tableResult
      .map { rows =>
        val headers = rows.getSchema.getFields.iterator().asScala.toList.map(_.getName)
        val values =
          rows.iterateAll().asScala.toList.map { row =>
            row
              .iterator()
              .asScala
              .toList
              .map(cell => scala.Option(cell.getValue()).map(_.toString).getOrElse("null"))
          }
        val result = prettyPrint(format, headers, values)
        result
      }
      .getOrElse("")
  }

  def show(format: String, rootServe: scala.Option[String]): Unit = {
    val output = rootServe.map(File(_, "extension.log"))
    output.foreach(_.append(s"Total Bytes Processed: $totalBytesProcessed bytes.\n"))
    println(s"Total Bytes Processed: $totalBytesProcessed bytes.")
    val res = prettyPrint(format)
    println(res)
  }
}
