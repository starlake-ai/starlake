package ai.starlake.serve.catalog

final case class CatalogConnection(
  name: String,
  `type`: String,
  engine: String,
  browsable: Boolean,
  reason: Option[String]
)

final case class CatalogSchema(name: String)

/** kind is "TABLE" or "VIEW" */
final case class CatalogTable(name: String, kind: String, remarks: Option[String])

final case class CatalogColumn(
  name: String,
  `type`: String,
  nullable: Boolean,
  precision: Option[Int],
  scale: Option[Int],
  remarks: Option[String]
)

final case class CatalogPreviewColumn(name: String, `type`: String)

final case class CatalogPreview(
  columns: List[CatalogPreviewColumn],
  rows: List[List[Any]],
  truncated: Boolean
)

object CatalogErrorCode {
  val UnknownConnection = "UNKNOWN_CONNECTION"
  val NotBrowsable = "NOT_BROWSABLE"
  val ConnectionFailed = "CONNECTION_FAILED"
  val QueryFailed = "QUERY_FAILED"
  val BadRequest = "BAD_REQUEST"
}

class CatalogException(val code: String, message: String, cause: Throwable = null)
    extends Exception(message, cause)
