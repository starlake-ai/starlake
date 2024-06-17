package ai.starlake.job.ingest

import ai.starlake.schema.model.{Attribute, Schema}

import java.sql.Timestamp
import java.util.regex.Pattern

case class RejectedRecord(
  jobid: String,
  timestamp: Timestamp,
  domain: String,
  schema: String,
  error: String,
  path: String
)

object RejectedRecord {
  val starlakeSchema = Schema(
    name = "rejected",
    pattern = Pattern.compile("ignore"),
    attributes = List(
      Attribute("jobid", "string"),
      Attribute("timestamp", "timestamp"),
      Attribute("domain", "string"),
      Attribute("schema", "string"),
      Attribute("error", "string"),
      Attribute("path", "string")
    ),
    None,
    None
  )
}
