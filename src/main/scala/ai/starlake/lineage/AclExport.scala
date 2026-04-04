package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{AccessControlEntry, RowLevelSecurity}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

class AclExport(schemaHandler: SchemaHandler)(implicit settings: Settings) extends LazyLogging {

  def exportToFile(config: AclConfig): Unit = {
    val content = buildYaml()
    val storageHandler = settings.storageHandler()
    storageHandler.write(content, new Path(config.outputPath))
    logger.info(s"ACL export written to ${config.outputPath}")
  }

  private case class GrantEntry(
    target: String,
    principals: Set[String],
    acl: List[AccessControlEntry],
    rls: List[RowLevelSecurity]
  )

  private def collectGrants(): List[GrantEntry] = {
    val domains = schemaHandler.domains()
    val tasks = schemaHandler.tasks()

    val domainGrants = domains.flatMap { domain =>
      val dbPrefix = schemaHandler.getDatabase(domain).map(_ + ".").getOrElse("")
      domain.tables
        .filter(t => t.acl.nonEmpty || t.rls.nonEmpty)
        .map { table =>
          val target = s"$dbPrefix${domain.finalName}.${table.finalName}"
          val principals = table.acl.flatMap(_.grants).toSet
          GrantEntry(target, principals, table.acl, table.rls)
        }
    }

    val taskGrants = tasks
      .filter(t => t.acl.nonEmpty || t.rls.nonEmpty)
      .map { task =>
        val dbPrefix = task.database.map(_ + ".").getOrElse("")
        val target = s"$dbPrefix${task.domain}.${task.table}"
        val principals = task.acl.flatMap(_.grants).toSet
        GrantEntry(target, principals, task.acl, task.rls)
      }

    domainGrants ++ taskGrants
  }

  private def buildYaml(): String = {
    val sb = new StringBuilder
    val grants = collectGrants()

    sb.append("grants:\n")

    grants.foreach { grant =>
      sb.append(s"  - target: ${grant.target}\n")

      if (grant.principals.nonEmpty) {
        sb.append("    principals:\n")
        grant.principals.toList.sorted.foreach { principal =>
          sb.append(s"      - $principal\n")
        }
      }

      if (grant.rls.nonEmpty) {
        sb.append("    rls:\n")
        grant.rls.foreach { rls =>
          sb.append(s"      - name: ${rls.name}\n")
          sb.append(s"        predicate: \"${rls.predicate}\"\n")
          if (rls.grants.nonEmpty) {
            sb.append("        grants:\n")
            rls.grants.toList.sorted.foreach { g =>
              sb.append(s"          - $g\n")
            }
          }
          if (rls.description.nonEmpty) {
            sb.append(s"        description: \"${rls.description}\"\n")
          }
        }
      }

      sb.append("\n")
    }

    if (grants.isEmpty) {
      sb.append("  []\n")
    }

    sb.toString()
  }
}