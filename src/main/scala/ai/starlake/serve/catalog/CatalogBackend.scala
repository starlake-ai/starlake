package ai.starlake.serve.catalog

trait CatalogBackend {
  def schemas(): List[CatalogSchema]
  def tables(schema: String): List[CatalogTable]
  def columns(schema: String, table: String): List[CatalogColumn]
  def preview(schema: String, table: String, limit: Int): CatalogPreview
}
