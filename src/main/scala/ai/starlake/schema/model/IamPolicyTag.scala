package ai.starlake.schema.model

case class IamPolicyTag(policyTag: String, members: List[String], role: String)

case class IamPolicyTags(iamPolicyTags: List[IamPolicyTag])
