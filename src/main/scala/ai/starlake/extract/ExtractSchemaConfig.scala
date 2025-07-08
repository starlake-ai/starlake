package ai.starlake.extract

case class ExtractSchemaConfig(
  extractConfig: String = "",
  all: Boolean = false,
  // List of tables to extract, if empty all tables are extracted domain.table
  tables: Seq[String] = Nil,
  outputDir: Option[String] = None,
  parallelism: Option[Int] = None,
  external: Boolean = false,
  connectionRef: Option[String] = None,
  accessToken: Option[String] = None,
  snakeCase: Boolean = false
)
