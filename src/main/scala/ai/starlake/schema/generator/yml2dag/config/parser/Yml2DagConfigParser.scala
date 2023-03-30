package ai.starlake.schema.generator.yml2dag.config.parser

import ai.starlake.schema.generator.yml2dag.config.Yml2DagConfig
import scopt.{OParser, OParserBuilder}

trait Yml2DagConfigParser {
  def name: String

  def parserBuilder(builder: OParserBuilder[Yml2DagConfig]): List[OParser[_, Yml2DagConfig]]
}
