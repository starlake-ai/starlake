package ai.starlake.schema.model

case class IamPolicyTag(policyTag: String, members: List[String], role: String) {
  def this() = this("", Nil, "") // Should never be called. Here for Jackson deserialization only
}

case class IamPolicyTags(iamPolicyTags: List[IamPolicyTag])
