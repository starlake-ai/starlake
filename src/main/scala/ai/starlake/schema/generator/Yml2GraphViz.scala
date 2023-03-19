package ai.starlake.schema.generator

import better.files.File
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Schema
import com.typesafe.scalalogging.LazyLogging

class Yml2GraphViz(schemaHandler: SchemaHandler) extends LazyLogging {

  val prefix = """
                 |digraph {
                 |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                 |node [shape=plain]
                 |rankdir=LR;
                 |
                 |
                 |""".stripMargin

  val aclPrefix = """
                 |digraph {
                 |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                 |
                 |
                 |""".stripMargin

  val suffix = """
                     |}
                     |""".stripMargin

  private def relatedTables(): List[String] = {
    schemaHandler.domains().flatMap(_.relatedTables())
  }

  def run(args: Array[String]): Unit = {
    Yml2GraphVizConfig.parse(args) match {
      case Some(config) =>
        if (config.acl) aclsAsDotFile(config)
        if (config.domains) relationsAsDotFile(config)
      case _ =>
        println(Yml2GraphVizConfig.usage())
    }
  }

  private def formatDotName(name: String) = {
    name.replaceAll("[^\\p{Alnum}]", "_")
  }

  private def tableAndAclAndRlsUsersAsDot() = {
    val aclTables = schemaHandler.domains().map(d => d.getFinalName() -> d.aclTables().toSet).toMap
    val aclTableGrants = aclTables.values.flatten
      .flatMap(_.acl)
      .flatMap(_.grants)
      .map(_.toLowerCase())
      .toSet

    val rlsTableGrants =
      rlsTables().values.flatten.flatMap(_._2).flatMap(_.grants).map(_.toLowerCase()).toSet

    val aclTasks = schemaHandler.jobs().values.flatMap(_.aclTasks())
    val rlsTasks = schemaHandler.jobs().values.flatMap(_.rlsTasks())

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

    usersAsDot(rlsTableGrants ++ aclTableGrants ++ rlsTaskGrants ++ aclTaskGrants)
  }

  private def usersAsDot(allGrants: Set[String]): String = {
    val allUsers = allGrants.filter(_.startsWith("user:")).map(_.substring("user:".length))
    val allGroups = allGrants.filter(_.startsWith("group:")).map(_.substring("group:".length))
    val allDomains = allGrants.filter(_.startsWith("domain:")).map(_.substring("domain:".length))
    val allSa =
      allGrants
        .map(_.replace("sa:", "serviceAccount:"))
        .filter(_.startsWith("serviceAccount:"))
        .map(_.substring("serviceAccount:".length))

    val formattedUsers = allUsers.map { user => s"""${formatDotName(user)}[label = "$user"]""" }
    val formattedGroups = allGroups.map { group =>
      s"""${formatDotName(group)}[label = "$group"]"""
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

  private def rlsTables() = {
    schemaHandler
      .domains()
      .map(d => d.getFinalName() -> d.rlsTables())
      .filter { case (domainName, rls) => rls.nonEmpty }
      .toMap
  }

  private def jobsAsDot(): String = {
    val allJobs = schemaHandler
      .jobs()
      .values

    val rlsAclTasks = allJobs.flatMap(_.aclTasks()) ++ allJobs.flatMap(_.rlsTasks()).toList

    val rlsAclTaskNames = rlsAclTasks
      .map { case desc =>
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
            s"""<tr><td port="0" bgcolor="darkgreen"><B><FONT color="white"> $table </FONT></B></td></tr>\n"""
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

  private def rlsAclTablesAsDot(): String = {
    val rlsTableNames: Map[String, Set[String]] = rlsTables().map { case (domain, rlsMap) =>
      domain -> rlsMap.keySet
    }

    val aclTableNames = schemaHandler
      .domains()
      .map(d => d.getFinalName() -> d.aclTables().toSet[Schema].map(x => x.getFinalName()))
      .toMap

    val tables: Map[String, Set[String]] = rlsTableNames ++ aclTableNames
    tables.toList
      .map { case (domain, tables) =>
        val tablesAsDot = tables.map { table =>
          val tableLabel = s"${domain}_$table"
          val header =
            s"""<tr><td port="0" bgcolor="darkgreen"><B><FONT color="white"> $table </FONT></B></td></tr>\n"""
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

  private def aclRelationsAsDot(): String = {
    val aclTables = schemaHandler.domains().map(d => d.getFinalName() -> d.aclTables().toSet).toMap
    val aclTablesRelations = aclTables.toList.flatMap { case (domainName, schemas) =>
      schemas.flatMap { schema =>
        val acls = schema.acl
        val schemaName = schema.getFinalName()
        acls.flatMap { ace =>
          ace.grants.map(userName => (userName, ace.role, schemaName, domainName))
        }
      }
    }

    val allJobs = schemaHandler
      .jobs()
      .values

    val aclAclTasks = allJobs.flatMap(_.aclTasks()) ++ allJobs.flatMap(_.rlsTasks()).toList
    val aclTaskRelations = aclAclTasks.toList.flatMap { desc =>
      desc.acl.flatMap { ace =>
        ace.grants.map(userName => (userName, ace.role, desc.table, desc.domain))
      }
    }

    val allRelations = aclTaskRelations ++ aclTablesRelations
    val dotAclRoles = allRelations.map { case (name, role, schema, domain) =>
      s"""${domain}_${schema}_acl_${formatDotName(role)} [shape=invhouse, label = "$role"]"""
    }.toSet

    val dotAclRolesRelations = allRelations.map { case (name, role, schema, domain) =>
      s"${domain}_${schema}_acl_${formatDotName(role)} -> ${domain}_$schema"
    }.toSet

    val dotAclRelations = allRelations.map { case (name, role, schema, domain) =>
      s"""${formatDotName(
          name.substring(name.indexOf(':') + 1)
        )} -> ${domain}_${schema}_acl_${formatDotName(role)}"""
    }.toSet

    mkstr(dotAclRoles.toList, ";\n") +
    mkstr(dotAclRolesRelations.toList, ";\n") +
    mkstr(dotAclRelations.toList, ";\n")
  }
  private def mkstr(list: List[String], sep: String) =
    if (list.isEmpty) ""
    else
      list.mkString("", sep, sep)

  private def rlsRelationsAsDot(): String = {
    val rlsTables =
      schemaHandler
        .domains()
        .map(d => d.getFinalName() -> d.rlsTables())
        .filter { case (domainName, rls) => rls.nonEmpty }
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

    val rlsTasks = schemaHandler.jobs().values.flatMap(_.rlsTasks())

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
    }.toList

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
    mkstr(dotRlsRelations.toList, ";\n")
  }

  private def aclsAsDotFile(config: Yml2GraphVizConfig) = {
    val result: String = aclAsDotString(config)
    config.outputDir match {
      case None => println(result)
      case Some(outputDir) =>
        val outputDirFile = File(outputDir)
        outputDirFile.createDirectories()
        val file = File(outputDirFile, "_acl.dot")
        file.overwrite(result)
    }
  }

  def aclAsDotString(config: Yml2GraphVizConfig): String = {
    schemaHandler.domains(config.reload)
    aclPrefix + rlsAclTablesAsDot() + jobsAsDot() + tableAndAclAndRlsUsersAsDot() + aclRelationsAsDot() + rlsRelationsAsDot() + suffix
  }

  private def relationsAsDotFile(config: Yml2GraphVizConfig): Unit = {
    val result: String = relationsAsDotString(config)
    config.outputDir match {
      case None => println(result)
      case Some(outputDir) =>
        val outputDirFile = File(outputDir)
        outputDirFile.createDirectories()
        val file = File(outputDirFile, "_domains.dot")
        file.overwrite(result)
    }
  }

  def relationsAsDotString(config: Yml2GraphVizConfig): String = {
    schemaHandler.domains(config.reload)
    val fkTables = relatedTables().map(_.toLowerCase).toSet
    val dots =
      schemaHandler.domains().map(_.asDot(config.includeAllAttributes, fkTables))
    prefix + dots.mkString("\n") + suffix
  }
}
