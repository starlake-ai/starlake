package ai.starlake.extract

case class ExtractSchemaConfig(
  extractConfig: String = "",
  outputDir: Option[String] = None,
  parallelism: Option[Int] = None
)
