package ai.starlake.schema.generator.yml2dag

import ai.starlake.schema.generator.yml2dag.command.{
  Yml2DagGenerateCommand,
  Yml2DagListCommand,
  Yml2DagShowCommand
}
import ai.starlake.schema.generator.yml2dag.config.{
  NoYml2DagConfig,
  Yml2DagConfig,
  Yml2DagGenerateConfig,
  Yml2DagListConfig,
  Yml2DagShowConfig
}
import ai.starlake.schema.handlers.SchemaHandler

object Yml2DagCommandFactory {
  def apply(config: Yml2DagConfig, schemaHandler: SchemaHandler) = {
    config match {
      case NoYml2DagConfig =>
        throw new RuntimeException("No command has been given. This should never happen")
      case config: Yml2DagGenerateConfig => new Yml2DagGenerateCommand(schemaHandler).run(config)
      case config: Yml2DagShowConfig     => new Yml2DagShowCommand().run(config)
      case config: Yml2DagListConfig     => new Yml2DagListCommand().run(config)
    }
  }
}
