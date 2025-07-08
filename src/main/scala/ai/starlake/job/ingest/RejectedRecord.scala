package ai.starlake.job.ingest

import ai.starlake.schema.model.{Attribute, SchemaInfo}

import java.sql.Timestamp
import java.util.regex.Pattern

case class RejectedRecord(
  jobid: String,
  timestamp: Timestamp,
  domain: String,
  schema: String,
  error: String,
  path: String
) {
  def asMap(): Map[String, Any] = {
    Map(
      "jobid"     -> jobid,
      "timestamp" -> timestamp,
      "domain"    -> domain,
      "schema"    -> schema,
      "error"     -> error,
      "path"      -> path
    )
  }
}

object RejectedRecord {
  val starlakeSchema = SchemaInfo(
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
