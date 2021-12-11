package ai.starlake.schema.model

case class AccessControlEntry(role: String, grants: List[String])
