package ai.starlake.schema.model

case class AccessControlEntry(role: String, grants: List[String] = Nil) {
  def this() = {
    this("", Nil)
    throw new Exception("Should never be called. Here to satisfy Jackson only")
  }
}
