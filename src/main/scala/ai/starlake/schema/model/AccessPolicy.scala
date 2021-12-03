package ai.starlake.schema.model

case class AccessPolicy(name: String, grants: Option[Set[String]])
