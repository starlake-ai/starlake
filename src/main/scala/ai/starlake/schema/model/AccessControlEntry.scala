package ai.starlake.schema.model

case class AccessControlEntry(role: String, grants: List[String] = Nil) {
  def this() = this("", Nil) // Should never be called. Here for Jackson deserialization only
}
