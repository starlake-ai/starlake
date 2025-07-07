package ai.starlake.schema.model

trait SecurityLevel extends Named {
  def grants: Set[String]
}
