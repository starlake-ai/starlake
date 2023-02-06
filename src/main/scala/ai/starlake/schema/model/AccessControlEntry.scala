package ai.starlake.schema.model

case class AccessControlEntry(role: String, grants: List[String] = Nil) extends Named {
  def this() = this("", Nil) // Should never be called. Here for Jackson deserialization only
  val name: String = role

  def compare(other: AccessControlEntry): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(this.name, this, other)

}
