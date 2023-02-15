package ai.starlake.schema.model

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JsonSerializer
import org.apache.hadoop.fs.Path

object Project {
  def compare(project1Path: Path, project2Path: Path)(settings: Settings): String = {
    val settings1 =
      settings.copy(comet =
        settings.comet.copy(metadata = new Path(project1Path, "metadata").toString)
      )

    val settings2 =
      settings.copy(comet =
        settings.comet.copy(metadata = new Path(project2Path, "metadata").toString)
      )

    val schemaHandler1 = new SchemaHandler(settings1.storageHandler)(settings1)
    val p1Domains =
      schemaHandler1
        .deserializedDomains(new Path(DatasetArea.metadata(settings1), "domains"))
        .flatMap { case (path, tryDomain) =>
          tryDomain.toOption
        }

    val schemaHandler2 = new SchemaHandler(settings1.storageHandler)(settings2)
    val p2Domains =
      schemaHandler2
        .deserializedDomains(new Path(DatasetArea.metadata(settings2), "domains"))
        .flatMap { case (path, tryDomain) =>
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
         |}""".stripMargin + "}"
  }
}
