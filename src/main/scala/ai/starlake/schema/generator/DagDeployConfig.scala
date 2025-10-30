package ai.starlake.schema.generator

case class DagDeployConfig(
  inputDir: Option[String] = None,
  outputDir: String,
  dagDir: Option[String] = None,
  clean: Boolean = false
)
