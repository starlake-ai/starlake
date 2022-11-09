package ai.starlake.schema.model

case class Env(env: Map[String, String] = Map.empty) {
  def this() = this(Map.empty) // Should never be called. Here for Jackson deserialization only
}
