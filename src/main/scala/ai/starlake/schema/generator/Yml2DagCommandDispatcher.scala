package ai.starlake.schema.generator

import ai.starlake.schema.generator.yml2dag._
import ai.starlake.schema.generator.yml2dag.config.parser.{
  Yml2DagGenerateConfigParser,
  Yml2DagListConfigParser,
  Yml2DagShowConfigParser
}
import ai.starlake.schema.generator.yml2dag.config.{NoYml2DagConfig, Yml2DagConfig}
import ai.starlake.schema.handlers.SchemaHandler
import scopt.OParser

import scala.collection.immutable

class Yml2DagCommandDispatcher(schemaHandler: SchemaHandler) {

  val availableSubConfigParser =
    List(
      new Yml2DagShowConfigParser(),
      new Yml2DagGenerateConfigParser(),
      new Yml2DagListConfigParser()
    )

  val command = Yml2DagCommandDispatcher.name

  val parser: OParser[Unit, Yml2DagConfig] = {
    val builder = OParser.builder[Yml2DagConfig]
    import builder._
    val commandDefinitions: immutable.Seq[OParser[_, Yml2DagConfig]] =
      availableSubConfigParser.flatMap { cmd =>
        cmd.parserBuilder(builder)
      }
    OParser.sequence(
      programName(s"starlake $command"),
      commandDefinitions :+ checkConfig {
        case NoYml2DagConfig => failure("Missing sub-command")
        case _               => success
      }: _*
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  private def parse(args: Seq[String]): Option[Yml2DagConfig] =
    OParser.parse(parser, args, NoYml2DagConfig)

  def run(args: Array[String]): Unit = {
    parse(args) match {
      case Some(config) => Yml2DagCommandFactory(config, schemaHandler)
      case _            => System.exit(1)
    }
  }
}

object Yml2DagCommandDispatcher {
  val name = "yml2dag"
}
