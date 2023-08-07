package ai.starlake.schema.model

case class ExternalSource(
  projects: List[ExternalDatabase]
)

case class ExternalDatabase(project: String, domains: List[ExternalDomain] = Nil) {
  def toMap(): Map[String, List[String]] =
    domains.map { domain => domain.name -> domain.tables }.toMap
}

case class ExternalDomain(name: String, tables: List[String] = Nil)
