package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{RowLevelSecurity, Schema}
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class AclDependencies(schemaHandler: SchemaHandler) extends LazyLogging {

  private val aclPrefix = """
                 |digraph {
                 |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                 |
                 |
                 |""".stripMargin

  private val suffix = """
                     |}
                     |""".stripMargin

  private def formatDotName(name: String) = {
    Utils.keepAlphaNum(name)
  }

  def run(args: Array[String]): Try[Unit] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    AclDependenciesCmd.run(args, schemaHandler).map(_ => ())
  }

  private def granteesAsDot(config: AclDependenciesConfig) = {
    val aclTables = schemaHandler
      .domains()
      .map(d => d.finalName -> d.aclTables(config).toSet)
      .toMap
      .filter(_._2.nonEmpty)
    val aclTableGrants = aclTables.values.flatten
      .flatMap(_.acl)
      .flatMap(_.grants)
      .map(_.toLowerCase())
      .toSet

    val rlsTableGrants =
      rlsTables(config).values.flatten.flatMap(_._2).flatMap(_.grants).map(_.toLowerCase()).toSet

    val aclTasks = schemaHandler.tasks().filter { task =>
      val taskToInclude = config.tables.exists(
        _.toLowerCase() == task.name.toLowerCase()
      )

      taskToInclude && task.acl.nonEmpty && (config.all || config.grantees
        .intersect(task.acl.flatMap(_.grants))
        .nonEmpty)
    }
    val rlsTasks = schemaHandler.tasks().filter { task =>
      val taskToInclude = config.tables.exists(
        _.toLowerCase() == task.name.toLowerCase()
      )

      taskToInclude && task.rls.nonEmpty &&
      (config.all || config.grantees.intersect(task.acl.flatMap(_.grants)).nonEmpty)
    }
    val aclTaskGrants =
      aclTasks
        .map(_.acl)
        .flatMap(_.map(_.grants))
        .flatten
        .toSet

    val rlsTaskGrants =
      rlsTasks
        .map(_.rls)
        .flatMap(_.map(_.grants))
        .flatten
        .toSet

    val allGrants = rlsTableGrants ++ aclTableGrants ++ rlsTaskGrants ++ aclTaskGrants
    val filteredGrants = allGrants.filter { grant =>
      config.all || config.grantees.contains(grant)
    }
    usersAsDot(filteredGrants)
  }

  private def usersAsDot(allGrants: Set[String]): String = {
    val notPrefixedUsers = allGrants.filterNot(_.contains(":"))
    val allUsers = notPrefixedUsers.union(
      allGrants.filter(_.startsWith("user:")).map(_.substring("user:".length))
    )
    val allGroups = allGrants.filter(_.startsWith("group:")).map(_.substring("group:".length))
    val allDomains = allGrants.filter(_.startsWith("domain:")).map(_.substring("domain:".length))
    val allSa =
      allGrants
        .map(_.replace("sa:", "serviceAccount:"))
        .filter(_.startsWith("serviceAccount:"))
        .map(_.substring("serviceAccount:".length))

    val formattedUsers = allUsers.map { user =>
      s"""${formatDotName(user)}[shape=plain label = "$user"]"""
    }
    val formattedGroups = allGroups.map { group =>
      s"""${formatDotName(group)}[shape=plain label = "$group"]"""
    }
    val formattedSa = allSa.map { sa => s"""${formatDotName(sa)}[label = "$sa"]""" }

    val usersSubgraph =
      if (allUsers.isEmpty) ""
      else
        s"""subgraph cluster_users {
                           |${formattedUsers.mkString("", ";\n", ";\n")}
                           |label = "Users";
                           |}\n""".stripMargin

    val groupsSubgraph =
      if (allGroups.isEmpty) ""
      else
        s"""subgraph cluster_groups {
           |${formattedGroups.mkString("", ";\n", ";\n")}
           |label = "Groups";
           |}\n""".stripMargin

    val domainsSubgraph =
      if (allDomains.isEmpty) ""
      else
        s"""subgraph cluster_domains {
           |${formattedGroups.mkString("", ";\n", ";\n")}
           |label = "Domains";
           |}\n""".stripMargin

    val saSubgraph =
      if (allSa.isEmpty) ""
      else
        s"""subgraph cluster_serviceAccounts {
           |${formattedSa.mkString("", ";\n", ";\n")}
           |label = "Service Accounts";
           |}\n""".stripMargin

    usersSubgraph + groupsSubgraph + saSubgraph + domainsSubgraph
  }

  /** @param config
    * @return
    *   Map of domain nad RLS tables in this domain: Map[domainName, Map[tableName,
    *   List[RowLevelSecurity]]]
    */
  private def rlsTables(
    config: AclDependenciesConfig
  ): Map[String, Map[String, List[RowLevelSecurity]]] = {
    if (config.tables.nonEmpty) {
      val domains = config.tables.map { t => t.split('.').head }
      schemaHandler
        .domains()
        .filter(d => domains.contains(d.finalName))
        .map(d => d.finalName -> d.rlsTables(config))
        .toMap
    } else {
      schemaHandler
        .domains()
        .map(d => d.finalName -> d.rlsTables(config))
        .filter { case (domainName, rls) => rls.nonEmpty }
        .toMap
    }
  }

  private def jobsAsDot(config: AclDependenciesConfig): String = {
    val allTasks = schemaHandler.tasks()
    val all = config.grantees
    val rlsAclTasks = allTasks.filter { desc =>
      config.all || config.grantees.intersect(desc.acl).nonEmpty || config.grantees
        .intersect(desc.rls)
        .nonEmpty
    }
    val rlsAclTaskNames = rlsAclTasks
      .filter { desc =>
        config.grantees.intersect(desc.acl).nonEmpty
      }
      .map { desc =>
        desc.domain -> desc.table
      }
      .groupBy(_._1)
      .mapValues(_.map { case (domain, table) => table }.toSet)

    val tasks: Map[String, Set[String]] = rlsAclTaskNames
    tasks.toList
      .map { case (domain, tables) =>
        val tablesAsDot = tables.map { table =>
          val tableLabel = s"${domain}_$table"
          val header =
            s"""<tr><td port="0" bgcolor="#00008B"><B><FONT color="black"> $table </FONT></B></td></tr>\n"""
          s"""
             |$tableLabel [label=<
             |<table border="0" cellborder="1" cellspacing="0">
             |""".stripMargin + header +
          """
              |</table>>];
              |""".stripMargin
        }

        s"""subgraph cluster_$domain {
           |node[shape = plain]
           |label = "$domain";
           |${tablesAsDot.mkString("\n")}
           |}""".stripMargin
      }
      .mkString(
        "",
        "\n",
        "\n"
      )
  }

  /** @param config
    * @return
    */
  private def rlsAclTablesAsDot(config: AclDependenciesConfig): String = {
    // All tables that have an RLS
    val rlsTableNames: Map[String, Set[String]] = rlsTables(config).map { case (domain, rlsMap) =>
      domain -> rlsMap.keySet
    }

    // All tables that have an ACL
    val aclTableNames: Map[String, Set[String]] = schemaHandler
      .domains()
      .map(d => d.finalName -> d.aclTables(config).toSet[Schema].map(_.finalName))
      .toMap
//      .filter { case (domainName, rls) => rls.nonEmpty }

    val tablesWithACLOrRLS: Map[String, Set[String]] = rlsTableNames ++ aclTableNames
    tablesWithACLOrRLS.toList
      .map { case (domain, tables) =>
        val tablesAsDot = tables.map { table =>
          val tableLabel = s"${domain}_$table"
          val header =
            s"""<tr><td port="0" bgcolor="white"><B><FONT color="black"> $table </FONT></B></td></tr>\n"""
          s"""
           |$tableLabel [label=<
           |<table border="0" cellborder="1" cellspacing="0">
           |""".stripMargin + header +
          """
            |</table>>];
            |""".stripMargin
        }

        s"""subgraph cluster_$domain {
         |node[shape = plain]
         |label = "$domain";
         |${tablesAsDot.mkString("\n")}
         |}""".stripMargin
      }
      .mkString(
        "",
        "\n",
        "\n"
      )
  }

  private def aclRelationsAsDot(config: AclDependenciesConfig): String = {
    val aclTables = schemaHandler
      .domains()
      .map(d => d.finalName -> d.aclTables(config).toSet)
      .filter(_._2.nonEmpty)
      .toMap

    val aclTablesRelations = aclTables.flatMap { case (domainName, schemas) =>
      schemas.flatMap { schema =>
        val acls = schema.acl
        val schemaName = schema.finalName
        acls
          .flatMap { ace =>
            ace.grants.map(userName => (userName, ace.role, schemaName, domainName))
          }
          .filter { case (userName, aceRole, schemaName, domainName) =>
            config.all || config.grantees.contains(userName)
          }
      }
    }

    val allTasks = schemaHandler.tasks().filter { task =>
      config.tables.exists(_.toLowerCase() == task.name.toLowerCase())
    }

    // val aclAclTasks = allTasks.filter(_.acl.nonEmpty) ++ allTasks.filter(_.rls.nonEmpty)
    val aclAclTasks = allTasks.filter { desc =>
      config.all ||
      config.grantees.intersect(desc.acl.flatMap(_.grants)).nonEmpty ||
      config.grantees.intersect(desc.rls.flatMap(_.grants)).nonEmpty
    }
    val aclTaskRelations = aclAclTasks.flatMap { desc =>
      desc.acl.flatMap { ace =>
        ace.grants.map(userName => (userName, ace.role, desc.table, desc.domain))
      }
    }

    val allRelations = aclTaskRelations ++ aclTablesRelations
    val dotAclRoles = allRelations.map { case (name, role, schema, domain) =>
      s"""${domain}_${schema}_acl_${formatDotName(role)} [shape=ellipse, label = "$role"]"""
    }.toSet

    val dotAclRolesRelations = allRelations.map { case (name, role, schema, domain) =>
      s"${domain}_${schema}_acl_${formatDotName(role)} -> ${domain}_$schema"
    }.toSet

    val dotAclRelations = allRelations.map { case (name, role, schema, domain) =>
      s"""${formatDotName(
          name.substring(name.indexOf(':') + 1)
        )} -> ${domain}_${schema}_acl_${formatDotName(role)}"""
    }.toSet

    val result =
      mkstr(dotAclRoles.toList, ";\n") +
      mkstr(dotAclRolesRelations.toList, ";\n") +
      mkstr(dotAclRelations.toList, ";\n")
    result
  }
  private def mkstr(list: List[String], sep: String): String =
    if (list.isEmpty) ""
    else
      list.mkString("", sep, sep)

  private def rlsRelationsAsDot(config: AclDependenciesConfig): String = {
    val rlsTables =
      schemaHandler
        .domains()
        .map(d => d.finalName -> d.rlsTables(config))
        .filter(_._2.nonEmpty)
        .toMap

    val rlsTableRelations = rlsTables.toList.flatMap { case (domainName, tablesMap) =>
      tablesMap.toList.flatMap { case (tableName, rls) =>
        rls.flatMap { r =>
          r.grants.map(userName =>
            (userName, r.name, Option(r.predicate).getOrElse("TRUE"), tableName, domainName)
          )
        }
      }
    }

    val allTasks = schemaHandler.tasks()
    val rlsTasks = allTasks.filter(_.rls.nonEmpty)

    val rlsTaskRelations = rlsTasks.flatMap { rlsTask =>
      rlsTask.rls
        .flatMap { r =>
          r.grants.map(userName =>
            (
              userName,
              r.name,
              Option(r.predicate).getOrElse("TRUE"),
              rlsTask.table,
              rlsTask.domain
            )
          )
        }
    }

    val allRlsRelations = rlsTableRelations ++ rlsTaskRelations
    val dotRlsRoles = allRlsRelations.map { case (userName, name, predicate, schema, domain) =>
      s"""${domain}_${schema}_rls_${formatDotName(name)} [shape=diamond, label = "$predicate"]"""
    }.toSet

    val dotRlsRolesRelations = allRlsRelations.map {
      case (userName, name, predicate, schema, domain) =>
        s"${domain}_${schema}_rls_${formatDotName(name)} -> ${domain}_$schema [style=dotted]"
    }.toSet

    val dotRlsRelations = allRlsRelations.map { case (userName, name, predicate, schema, domain) =>
      s"""${formatDotName(
          userName.substring(userName.indexOf(':') + 1)
        )} -> ${domain}_${schema}_rls_${formatDotName(name)} [style=dotted]"""
    }

    mkstr(dotRlsRoles.toList, ";\n") +
    mkstr(dotRlsRolesRelations.toList, ";\n") +
    mkstr(dotRlsRelations, ";\n")
  }

  def aclsAsDotFile(config: AclDependenciesConfig): Unit = {
    val domains = schemaHandler.domains(reload = config.reload)
    val configWithFinalTables =
      if (config.tables.length == 1 && !config.tables.head.contains('.')) {
        config.copy(tables = domains.flatMap(_.tables).map(_.finalName))
      } else {
        config
      }

    val dotStr =
      aclPrefix +
      rlsAclTablesAsDot(configWithFinalTables) +
      jobsAsDot(configWithFinalTables) +
      granteesAsDot(configWithFinalTables) +
      aclRelationsAsDot(configWithFinalTables) +
      rlsRelationsAsDot(configWithFinalTables) + suffix
    if (configWithFinalTables.svg)
      Utils.dot2Svg(configWithFinalTables.outputFile, dotStr)
    else if (configWithFinalTables.png)
      Utils.dot2Png(configWithFinalTables.outputFile, dotStr)
    else
      Utils.save(configWithFinalTables.outputFile, dotStr)
  }
}
