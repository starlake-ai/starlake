package ai.starlake.extract.spi

import ai.starlake.config.Settings
import ai.starlake.extract.impl.openapi.OpenAPISchemaExtractor
import ai.starlake.extract.{ExtractSchemaConfig, ExtractSchemas}

/** Factory object for creating instances of SchemaExtractor based on provided configuration.
  */
object SchemaExtractorFactory {
  def get(
    extractSchemaCliConfig: ExtractSchemaConfig,
    extractSchemas: ExtractSchemas
  )(implicit settings: Settings): SchemaExtractor = {
    val connection = resolveConnection(extractSchemaCliConfig, extractSchemas, settings)
    extractSchemas.openAPI match {
      case Some(openAPIConfig) =>
        new OpenAPISchemaExtractor(openAPIConfig, connection, extractSchemas.sanitizeAttributeName)
      case None =>
        throw new RuntimeException(
          "Could not create an instance of SchemaExtractor with the given information"
        )
    }
  }

  private def resolveConnection(
    extractSchemaCliConfig: ExtractSchemaConfig,
    extractSchemas: ExtractSchemas,
    settings: Settings
  ) = {
    extractSchemaCliConfig.connectionRef
      .orElse(extractSchemas.connectionRef)
      .map(settings.appConfig.getConnection)
      .getOrElse(settings.appConfig.getDefaultConnection())
  }
}
