package ai.starlake.schema.model

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.ProjectCompareConfig
import ai.starlake.schema.handlers.SchemaHandler
import org.apache.hadoop.fs.Path

object Project {
  def compare(config: ProjectCompareConfig)(implicit settings: Settings): ProjectDiff =
    compare(new Path(config.project1), new Path(config.project2))

  def compare(project1Path: Path, project2Path: Path)(implicit settings: Settings): ProjectDiff = {
    val settings1 =
      settings.copy(comet =
        settings.comet.copy(metadata = new Path(project1Path, "metadata").toString)
      )

    val settings2 =
      settings.copy(comet =
        settings.comet.copy(metadata = new Path(project2Path, "metadata").toString)
      )

    val schemaHandler1 = new SchemaHandler(settings1.storageHandler())(settings1)
    val schemaHandler2 = new SchemaHandler(settings1.storageHandler())(settings2)

    ProjectDiff(
      project1Path.toString,
      project2Path.toString,
      domainsDiff(settings1, settings2, schemaHandler1, schemaHandler2),
      jobsDiff(settings1, settings2, schemaHandler1, schemaHandler2)
    )
  }

  private def domainsDiff(
    settings1: Settings,
    settings2: Settings,
    schemaHandler1: SchemaHandler,
    schemaHandler2: SchemaHandler
  ): DomainsDiff = {
    val p1Domains =
      schemaHandler1
        .deserializedDomains(new Path(DatasetArea.metadata(settings1), "load"))
        .flatMap { case (path, tryDomain) =>
          tryDomain.toOption
        }

    val p2Domains =
      schemaHandler2
        .deserializedDomains(new Path(DatasetArea.metadata(settings2), "load"))
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

    val updatedDomainsDiff: List[DomainDiff] = commonDomains.flatMap { case (existing, incoming) =>
      Domain.compare(existing, incoming).toOption
    }
    DomainsDiff(addedDomains.map(_.name), deletedDomains.map(_.name), updatedDomainsDiff)
  }

  private def jobsDiff(
    settings1: Settings,
    settings2: Settings,
    schemaHandler1: SchemaHandler,
    schemaHandler2: SchemaHandler
  ): JobsDiff = {
    val p1Jobs =
      schemaHandler1
        .deserializedJobs(new Path(DatasetArea.metadata(settings1), "transform"))
        .flatMap { case (path, tryDomain) =>
          tryDomain.toOption
        }

    val p2Jobs =
      schemaHandler2
        .deserializedJobs(new Path(DatasetArea.metadata(settings2), "transform"))
        .flatMap { case (path, tryDomain) =>
          tryDomain.toOption
        }

    val (addedJobs, deletedJobs, existingCommonJobs) =
      AnyRefDiff.partitionNamed(p1Jobs, p2Jobs)

    val commonJobs: List[(AutoJobDesc, AutoJobDesc)] = existingCommonJobs.map { job =>
      (
        job,
        p2Jobs
          .find(_.name.toLowerCase() == job.name.toLowerCase())
          .getOrElse(throw new Exception("Should not happen"))
      )
    }

    val updatedJobsDiff: List[JobDiff] = commonJobs.flatMap { case (existing, incoming) =>
      AutoJobDesc.compare(existing, incoming).toOption
    }
    JobsDiff(addedJobs.map(_.name), deletedJobs.map(_.name), updatedJobsDiff)
  }
}
