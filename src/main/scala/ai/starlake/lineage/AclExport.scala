package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.YamlSerde
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.jdk.CollectionConverters._

class AclExport(schemaHandler: SchemaHandler)(implicit settings: Settings) extends LazyLogging {

  def exportToFile(config: AclConfig): Unit = {
    val content = buildYaml()
    val storageHandler = settings.storageHandler()
    storageHandler.write(content, new Path(config.outputPath))
    logger.info(s"ACL export written to ${config.outputPath}")
  }

  private def collectGrants(): List[Map[String, Any]] = {
    val domains = schemaHandler.domains()
    val tasks = schemaHandler.tasks()

    val domainGrants = domains.flatMap { domain =>
      val dbPrefix = schemaHandler.getDatabase(domain).map(_ + ".").getOrElse("")
      domain.tables
        .filter(t => t.acl.nonEmpty || t.rls.nonEmpty)
        .map { table =>
          val target = s"$dbPrefix${domain.finalName}.${table.finalName}"
          val principals = table.acl.flatMap(_.grants).toList.sorted
          val entry = scala.collection.mutable.LinkedHashMap[String, Any](
            "target" -> target
          )
          if (principals.nonEmpty) {
            entry("principals") = principals.asJava
          }
          if (table.rls.nonEmpty) {
            entry("rls") = table.rls.map { rls =>
              val rlsMap = scala.collection.mutable.LinkedHashMap[String, Any](
                "name"      -> rls.name,
                "predicate" -> rls.predicate
              )
              if (rls.grants.nonEmpty) {
                rlsMap("grants") = rls.grants.toList.sorted.asJava
              }
              if (rls.description.nonEmpty) {
                rlsMap("description") = rls.description
              }
              rlsMap.asJava
            }.asJava
          }
          entry.toMap
        }
    }

    val taskGrants = tasks
      .filter(t => t.acl.nonEmpty || t.rls.nonEmpty)
      .map { task =>
        val dbPrefix = task.database.map(_ + ".").getOrElse("")
        val target = s"$dbPrefix${task.domain}.${task.table}"
        val principals = task.acl.flatMap(_.grants).toList.sorted
        val entry = scala.collection.mutable.LinkedHashMap[String, Any](
          "target" -> target
        )
        if (principals.nonEmpty) {
          entry("principals") = principals.asJava
        }
        if (task.rls.nonEmpty) {
          entry("rls") = task.rls.map { rls =>
            val rlsMap = scala.collection.mutable.LinkedHashMap[String, Any](
              "name"      -> rls.name,
              "predicate" -> rls.predicate
            )
            if (rls.grants.nonEmpty) {
              rlsMap("grants") = rls.grants.toList.sorted.asJava
            }
            if (rls.description.nonEmpty) {
              rlsMap("description") = rls.description
            }
            rlsMap.asJava
          }.asJava
        }
        entry.toMap
      }

    domainGrants ++ taskGrants
  }

  private def buildYaml(): String = {
    val grants = collectGrants()
    val root = Map("grants" -> grants.map(_.asJava).asJava)
    YamlSerde.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root.asJava)
  }
}
