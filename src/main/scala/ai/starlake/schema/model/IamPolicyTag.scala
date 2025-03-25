package ai.starlake.schema.model

import com.fasterxml.jackson.annotation.JsonCreator

case class IamPolicyTag(
  policyTag: String,
  members: List[String],
  role: Option[String] = Some("roles/datacatalog.categoryFineGrainedReader")
) {
  @JsonCreator
  def this() = this(
    "",
    Nil,
    Some("roles/datacatalog.categoryFineGrainedReader")
  ) // Should never be called. Here for Jackson deserialization only
}

case class IamPolicyTags(iamPolicyTags: List[IamPolicyTag])
