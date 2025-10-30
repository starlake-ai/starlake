package ai.starlake.schema.generator

case class DagDeployConfig(
  inputDir: Option[String] = None,
  outputDir: String,
  clean: Boolean = false
)
