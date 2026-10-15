package ai.starlake.serve.catalog

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.schema.model.ConnectionType

import scala.util.Try

object CatalogService {
  val DefaultPreviewLimit = 100
  val MaxPreviewLimit = 1000
}

class CatalogService(connections: Map[String, ConnectionInfo])(implicit settings: Settings) {

  def listConnections(): List[CatalogConnection] =
    connections.toList.sortBy(_._1).map { case (name, info) =>
      val reason = notBrowsableReason(info)
      CatalogConnection(
        name = name,
        `type` = info.`type`.value,
        engine = Try(info.getJdbcEngineName().toString).getOrElse("unknown"),
        browsable = reason.isEmpty,
        reason = reason
      )
    }

  def listSchemas(connection: String): List[CatalogSchema] = backend(connection).schemas()

  def listTables(connection: String, schema: String): List[CatalogTable] =
    backend(connection).tables(schema)

  def listColumns(connection: String, schema: String, table: String): List[CatalogColumn] =
    backend(connection).columns(schema, table)

  def preview(connection: String, schema: String, table: String, limit: Int): CatalogPreview = {
    val effective = math.min(math.max(limit, 1), CatalogService.MaxPreviewLimit)
    backend(connection).preview(schema, table, effective)
  }

  /** Masks the credentials of every known connection in a message. */
  def scrub(message: String): String =
    CatalogScrubber.scrub(message, connections.values.flatMap(_.options).toMap)

  private def notBrowsableReason(info: ConnectionInfo): Option[String] = {
    val isSpark = Try(info.isSpark()).getOrElse(false)
    if (info.isBigQuery()) None
    else if (info.`type` == ConnectionType.FS || isSpark)
      Some("Spark and filesystem connections cannot be browsed")
    else if (info.`type` == ConnectionType.JDBC) None
    else Some(s"Connections of type ${info.`type`.value} cannot be browsed")
  }

  private def resolve(name: String): ConnectionInfo = {
    val info = connections.getOrElse(
      name,
      throw new CatalogException(CatalogErrorCode.UnknownConnection, s"Unknown connection '$name'")
    )
    notBrowsableReason(info).foreach { reason =>
      throw new CatalogException(CatalogErrorCode.NotBrowsable, reason)
    }
    info
  }

  protected def backend(name: String): CatalogBackend = {
    val info = resolve(name)
    if (info.isBigQuery()) new BigQueryCatalogBackend(name, info)
    else new JdbcCatalogBackend(info)
  }
}
