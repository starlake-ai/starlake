package ai.starlake.schema.generator.yml2dag.config.parser

import ai.starlake.schema.generator.yml2dag.config.{Yml2DagConfig, Yml2DagListConfig}
import com.typesafe.scalalogging.LazyLogging
import scopt.{OParser, OParserBuilder}

class Yml2DagListConfigParser extends LazyLogging with Yml2DagConfigParser {

  override val name = Yml2DagListConfigParser.name

  override def parserBuilder(
    builder: OParserBuilder[Yml2DagConfig]
  ): List[OParser[_, Yml2DagConfig]] = {
    List(
      builder
        .cmd(name)
        .text("List all templates available to the user inside dags folder and starlake internals.")
        .action((_, _) => Yml2DagListConfig())
    )
  }
}

object Yml2DagListConfigParser {
  val name = "list"
}
