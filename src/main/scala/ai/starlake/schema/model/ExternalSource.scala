package ai.starlake.schema.model

import ai.starlake.config.Settings.latestSchemaVersion
import com.fasterxml.jackson.annotation.JsonCreator

case class ExternalDesc(
  version: Int,
  external: ExternalSource
) {
  @JsonCreator
  def this() = this(latestSchemaVersion, ExternalSource(None))
}

case class ExternalSource(
  projects: Option[List[ExternalDatabase]]
) {
  @JsonCreator
  def this() = this(None)
}

//TODO: should we rename "project" as database instead?
case class ExternalDatabase(project: String, domains: Option[List[ExternalDomain]] = None) {
  @JsonCreator
  def this() = this("", None)

  def shouldExcludeTable(
    domainName: String,
    finalTableName: String,
    excludeTables: List[Domain]
  ): Boolean = {
    excludeTables.find(_.finalName.toLowerCase() == domainName.toLowerCase()) match {
      case Some(domain) =>
        domain.tables.find(_.finalName.toLowerCase() == finalTableName.toLowerCase()) match {
          case Some(_) => true
          case None    => false
        }
      case None => false
    }
  }

  def toMap(excludeTables: List[Domain]): Map[String, List[String]] = {
    val domainsList = domains.getOrElse(Nil)
    if (domainsList.nonEmpty && domainsList.head.name == "*") {
      Map.empty
    } else {
      domainsList.map { externalDomain =>
        if (externalDomain.tables.nonEmpty && externalDomain.tables.head == "*")
          externalDomain.name -> Nil
        else
          externalDomain.name ->
          externalDomain.tables
            .filter(externalTable =>
              !shouldExcludeTable(externalDomain.name, externalTable, excludeTables)
            )

      }.toMap
    }
  }
}

case class ExternalDomain(name: String, tables: List[String] = Nil) {
  @JsonCreator
  def this() = this("", Nil)
}
