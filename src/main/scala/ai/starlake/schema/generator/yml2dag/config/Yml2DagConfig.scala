package ai.starlake.schema.generator.yml2dag.config

import ai.starlake.schema.generator.Yml2DagCommandDispatcher
import ai.starlake.utils.CliConfig
import scopt.OParser

sealed trait Yml2DagConfig
case object NoYml2DagConfig extends Yml2DagConfig
case class Yml2DagGenerateConfig(domainTemplatePath: Option[String] = None) extends Yml2DagConfig

case class Yml2DagShowConfig(domainTemplatePath: Option[String] = None) extends Yml2DagConfig

case class Yml2DagListConfig() extends Yml2DagConfig

case object Yml2DagConfigForMain extends CliConfig[Yml2DagConfig] {
  override def parser: OParser[Unit, Yml2DagConfig] = throw new NotImplementedError()

  override def parse(args: Seq[String]): Option[Yml2DagConfig] = throw new NotImplementedError()

  override def command: String = Yml2DagCommandDispatcher.name
}
