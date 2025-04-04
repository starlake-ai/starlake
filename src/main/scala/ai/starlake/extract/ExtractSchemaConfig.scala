package ai.starlake.extract

case class ExtractSchemaConfig(
  extractConfig: String = "",
  all: Boolean = false,
  tables: Seq[String] = Nil,
  outputDir: Option[String] = None,
  parallelism: Option[Int] = None,
  external: Boolean = false,
  connectionRef: Option[String] = None,
  accessToken: Option[String] = None,
  snakeCase: Boolean = false
)
