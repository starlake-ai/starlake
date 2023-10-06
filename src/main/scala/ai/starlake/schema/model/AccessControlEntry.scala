package ai.starlake.schema.model

case class AccessControlEntry(role: String, grants: Set[String] = Set.empty, name: String = "")
    extends Named {

  def this() = this("", Set.empty) // Should never be called. Here for Jackson deserialization only

  def compare(other: AccessControlEntry): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(this.role, this, other)

}
