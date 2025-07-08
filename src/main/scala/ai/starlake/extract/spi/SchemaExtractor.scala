package ai.starlake.extract.spi

import ai.starlake.config.Settings
import ai.starlake.schema.model.DomainInfo

trait SchemaExtractor {

  /** Interface to implement when we want to provide a schema extractor connector.
    *
    * @return
    *   an Iterable collection of Domain objects representing the extracted schema information
    */
  def extract()(implicit settings: Settings): Iterable[DomainInfo]
}
