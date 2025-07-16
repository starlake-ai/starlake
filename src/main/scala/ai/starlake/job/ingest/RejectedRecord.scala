package ai.starlake.job.ingest

import ai.starlake.schema.model.{SchemaInfo, TableAttribute}

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
      TableAttribute("jobid", "string"),
      TableAttribute("timestamp", "timestamp"),
      TableAttribute("domain", "string"),
      TableAttribute("schema", "string"),
      TableAttribute("error", "string"),
      TableAttribute("path", "string")
    ),
    None,
    None
  )
}
