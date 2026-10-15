package ai.starlake.serve.catalog

import ai.starlake.config.{ConnectionInfo, Settings}

class BigQueryCatalogBackend(connectionName: String, connectionInfo: ConnectionInfo)(implicit
  settings: Settings
) extends CatalogBackend {
  private def unsupported =
    throw new CatalogException(CatalogErrorCode.NotBrowsable, "BigQuery browsing lands in Task 5")
  override def schemas(): List[CatalogSchema] = unsupported
  override def tables(schema: String): List[CatalogTable] = unsupported
  override def columns(schema: String, table: String): List[CatalogColumn] = unsupported
  override def preview(schema: String, table: String, limit: Int): CatalogPreview = unsupported
}
