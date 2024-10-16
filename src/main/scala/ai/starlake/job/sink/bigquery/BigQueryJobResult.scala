package ai.starlake.job.sink.bigquery

import ai.starlake.utils.{JobResult, JsonSerializer}
import com.google.cloud.bigquery._
import com.google.gson.Gson

import scala.jdk.CollectionConverters._

case class BigQueryJobResult(
  tableResult: scala.Option[TableResult],
  totalBytesProcessed: Long,
  job: scala.Option[Job]
) extends JobResult {

  private def flatten(fieldList: List[Field], parentPath: String): List[Map[String, String]] = {
    fieldList.flatMap { field =>
      val level = parentPath.count(_ == '/')
      val space = " " * 4 * level
      val hasSubFields = scala.Option(field.getSubFields).isDefined && !field.getSubFields.isEmpty
      val fieldName = space + field.getName
      val path = if (parentPath.isEmpty) fieldName else parentPath + "/" + fieldName
      val fieldMap =
        Map(
          "path"       -> path,
          "field_name" -> fieldName,
          "type"       -> field.getType.toString,
          "mode"       -> field.getMode.toString,
          "default"    -> scala.Option(field.getDefaultValueExpression).getOrElse(""),
          "policy_tags" -> scala
            .Option(field.getPolicyTags)
            .map(_.getNames.asScala.mkString(","))
            .getOrElse(""),
          "description" -> scala.Option(field.getDescription).getOrElse("")
        )
      if (!hasSubFields) {
        List(fieldMap)
      } else {
        List(fieldMap) ++ flatten(field.getSubFields.asScala.toList, path)
      }
    }
  }

  override def asMap(): List[Map[String, Any]] = {
    if (this.totalBytesProcessed < 0) {
      // The result is the schema of the table
      tableResult
        .map { tableResult =>
          val fieldList = tableResult.getSchema.getFields.iterator().asScala.toList
          flatten(fieldList, "")
        }
        .getOrElse(Nil)
    } else {
      tableResult
        .map { rows =>
          val headers: List[Field] = rows.getSchema.getFields.iterator().asScala.toList
          val values = rows.iterateAll().asScala.toList.map { row =>
            val fields = row
              .iterator()
              .asScala
              .toList
            asMap(fields, headers)
          }
          values
        }
        .getOrElse(Nil)
      /*
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
       */
    }
  }

  override def prettyPrint(format: String, dryRun: Boolean = false): String = {
    if (dryRun) {
      val map = Map("totalBytesProcessed" -> totalBytesProcessed.toString).asJava
      val json = new Gson().toJson(map)
      json
    } else {
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
          val result =
            if (format == "json-array") {
              val result =
                tableResult
                  .map { rows =>
                    val headers: List[Field] = rows.getSchema.getFields.iterator().asScala.toList
                    val values = rows.iterateAll().asScala.toList.map { row =>
                      val fields = row
                        .iterator()
                        .asScala
                        .toList
                      asMap(fields, headers)
                    }
                    values
                  }
                  .getOrElse(Nil)
              JsonSerializer.mapper.writeValueAsString(result)
            } else {
              prettyPrint(format, headers, values)
            }
          result
        }
        .getOrElse("")
    }
  }

  def asMap(fields: List[FieldValue], headers: List[Field]): Map[String, Any] = {
    val result =
      fields
        .zip(headers)
        .map { case (fieldValue, header) =>
          val attribute = fieldValue.getAttribute
          val headerName = header.getName
          val obj =
            attribute match {
              case FieldValue.Attribute.PRIMITIVE =>
                headerName -> scala.Option(fieldValue.getValue).map(_.toString).getOrElse("NULL")
              case FieldValue.Attribute.RECORD =>
                val record = fieldValue.getValue.asInstanceOf[FieldValueList]
                val subFieldValues = record.iterator().asScala.toList
                val subHeaders =
                  scala.Option(header.getSubFields.iterator()).map(_.asScala.toList).getOrElse(Nil)
                val value = asMap(subFieldValues, subHeaders)
                headerName -> value
              case FieldValue.Attribute.REPEATED =>
                val record = fieldValue.getValue.asInstanceOf[FieldValueList]
                val subFieldValues = record.iterator().asScala.toList
                val valueList =
                  if (header.getSubFields == null) {
                    val valueList = subFieldValues
                      .map(subField =>
                        scala.Option(subField.getValue).map(_.toString).getOrElse("NULL")
                      )
                    valueList
                  } else {
                    val subHeaders =
                      scala
                        .Option(header.getSubFields.iterator())
                        .map(_.asScala.toList)
                        .getOrElse(Nil)
                    val valueList = subFieldValues.map { subField =>
                      val record = subField.getValue.asInstanceOf[FieldValueList]
                      val subFieldValues = record.iterator().asScala.toList
                      val value = asMap(subFieldValues, subHeaders)
                      value
                    }
                    valueList
                  }
                headerName -> valueList
              case FieldValue.Attribute.RANGE =>
                val value =
                  scala.Option(fieldValue.getRangeValue).map(_.getValues).getOrElse("NULL")
                headerName -> value
            }
          obj
        }
        .toMap
    result
  }

  def show(format: String): Unit = {
    println(s"Total Bytes Processed: $totalBytesProcessed bytes.")
    val res = prettyPrint(format)
    println(res)
  }
}
