package ai.starlake.schema.generator.yml2dag

sealed trait Yml2DagTemplate {
  def path: String
}

case class DomainTemplate(path: String) extends Yml2DagTemplate
