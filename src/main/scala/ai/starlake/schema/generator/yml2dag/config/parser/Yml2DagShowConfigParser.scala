package ai.starlake.schema.generator.yml2dag.config.parser

import ai.starlake.schema.generator.yml2dag.config.{Yml2DagConfig, Yml2DagShowConfig}
import com.typesafe.scalalogging.LazyLogging
import scopt.{OParser, OParserBuilder}

class Yml2DagShowConfigParser extends LazyLogging with Yml2DagConfigParser {

  override val name = Yml2DagShowConfigParser.name

  override def parserBuilder(
    builder: OParserBuilder[Yml2DagConfig]
  ): List[OParser[_, Yml2DagConfig]] = {
    import builder._
    List(
      builder
        .cmd(name)
        .text("Print the content of the template")
        .action((_, _) => Yml2DagShowConfig())
        .children(
          opt[String]("domain-template")
            .action {
              case (dt, config: Yml2DagShowConfig) =>
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
            case Yml2DagShowConfig(Some(_)) => success
            case Yml2DagShowConfig(None)    => failure("domain-template must be defined")
            case _                          => success
          }
        )
    )
  }
}

object Yml2DagShowConfigParser {
  val name = "show"
}
