package ai.starlake.schema.model

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JsonSerializer
import org.apache.hadoop.fs.Path

object Project {
  def compare(project1Path: Path, project2Path: Path)(implicit schemaHandler: SchemaHandler) = {
    val p1Domains =
      schemaHandler.deserializedDomains(project1Path).flatMap { case (path, tryDomain) =>
        tryDomain.toOption
      }
    val p2Domains =
      schemaHandler.deserializedDomains(project2Path).flatMap { case (path, tryDomain) =>
        tryDomain.toOption
      }

    val (addedDomains, deletedDomains, existingCommonDomains) =
      AnyRefDiff.partitionNamed(p1Domains, p2Domains)

    val commonDomains: List[(Domain, Domain)] = existingCommonDomains.map { domain =>
      (
        domain,
        p2Domains
          .find(_.name.toLowerCase() == domain.name.toLowerCase())
          .getOrElse(throw new Exception("Should not happen"))
      )
    }

    val updatedDomainsDiffAsJson: List[String] = commonDomains.flatMap {
      case (existing, incoming) =>
        Domain.compare(existing, incoming).toOption
    }
    s"""{ "projects": ["${project1Path}", "${project2Path}"], """ +
    s"""
         |"domains": {
         |  "added": ${JsonSerializer.serializeObject(addedDomains.map(_.name))}
         |  , "deleted": ${JsonSerializer.serializeObject(deletedDomains.map(_.name))}
         |  , "updated": [${updatedDomainsDiffAsJson.mkString(",")}]
         |},""".stripMargin + "]}"
  }
}
