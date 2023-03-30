package ai.starlake.schema.generator.yml2dag.config.parser

import ai.starlake.schema.generator.yml2dag.config.{Yml2DagConfig, Yml2DagGenerateConfig}
import com.typesafe.scalalogging.LazyLogging
import scopt.{OParser, OParserBuilder}

class Yml2DagGenerateConfigParser extends LazyLogging with Yml2DagConfigParser {

  override val name = Yml2DagGenerateConfigParser.name

  override def parserBuilder(
    builder: OParserBuilder[Yml2DagConfig]
  ): List[OParser[_, Yml2DagConfig]] = {
    import builder._
    List(
      builder
        .cmd(name)
        .text(
          "Generate dags based on the given templates. To create your own template, you can get more insight by using the `show` command."
        )
        .action((_, _) => Yml2DagGenerateConfig())
        .children(
          opt[String]("domain-template")
            .action {
              case (dt, config: Yml2DagGenerateConfig) =>
                config.copy(domainTemplatePath = Some(dt))
              case (_, config) =>
                throw new RuntimeException(
                  s"Missing handle of config type ${config.getClass} but should not happen"
                )
            }
            .text(
              "Define domain template to use. May be a user path or a template name returned by `list` command"
            ),
          checkConfig {
            case Yml2DagGenerateConfig(Some(_)) => success
            case Yml2DagGenerateConfig(None)    => failure("domain-template must be defined")
            case _                              => success
          }
        )
    )
  }
}

object Yml2DagGenerateConfigParser {
  val name = "generate"
}
