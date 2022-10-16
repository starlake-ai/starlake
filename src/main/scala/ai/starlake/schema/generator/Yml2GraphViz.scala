package ai.starlake.schema.generator

import better.files.File
import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Schema
import com.typesafe.config.ConfigFactory
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
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Yml2GraphVizConfig.parse(args) match {
      case Some(config) if !config.acl.getOrElse(false) =>
        asDotRelations(config)
      case Some(config) if config.acl.getOrElse(false) =>
        asDotRelations(config)
        aclsAsDot(config)
      case _ =>
        println(Yml2GraphVizConfig.usage())
    }
  }

  private def formatDotName(name: String) = {
    name.replaceAll("[^\\p{Alnum}]", "_")
  }

  private def usersAsDot(): String = {
    val aclTables = schemaHandler.domains().map(d => d.getFinalName() -> d.aclTables().toSet).toMap
    val aclGrants = aclTables.values.flatten
      .flatMap(_.acl.getOrElse(Nil))
      .flatMap(_.grants)
      .map(_.toLowerCase())
      .toSet

    val rlsTables =
      schemaHandler
        .domains()
        .map(d => d.getFinalName() -> d.rlsTables())
        .filter { case (domainName, rls) => rls.nonEmpty }
        .toMap

    val rlsGrants =
      rlsTables.values.flatten.flatMap(_._2).flatMap(_.grants).map(_.toLowerCase()).toSet

    val allGrants = rlsGrants ++ aclGrants
    val allUsers = allGrants.filter(_.startsWith("user:")).map(_.substring("user:".length))
    val allGroups = allGrants.filter(_.startsWith("group:")).map(_.substring("group:".length))
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

    val saSubgraph =
      if (allSa.isEmpty) ""
      else
        s"""subgraph cluster_serviceAccounts {
           |${formattedSa.mkString("", ";\n", ";\n")}
           |label = "Service Accounts";
           |}\n""".stripMargin

    usersSubgraph + groupsSubgraph + saSubgraph
  }

  private def tablesAsDot(): String = {
    val rlsTables =
      schemaHandler
        .domains()
        .map(d => d.getFinalName() -> d.rlsTables())
        .filter { case (domainName, rls) => rls.nonEmpty }
        .toMap

    val rlsTableNames: Map[String, Set[String]] = rlsTables.map { case (domain, rlsMap) =>
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
    val aclRelations = aclTables.toList.flatMap { case (domainName, schemas) =>
      schemas.flatMap { schema =>
        val acls = schema.acl.getOrElse(Nil)
        val schemaName = schema.getFinalName()
        acls.flatMap { ace =>
          ace.grants.map(userName => (userName, ace.role, schemaName, domainName))
        }
      }
    }

    val dotAclRoles = aclRelations.map { case (name, role, schema, domain) =>
      s"""${domain}_${schema}_acl_${formatDotName(role)} [shape=invhouse, label = "$role"]"""
    }.toSet

    val dotAclRolesRelations = aclRelations.map { case (name, role, schema, domain) =>
      s"${domain}_${schema}_acl_${formatDotName(role)} -> ${domain}_$schema"
    }.toSet

    val dotAclRelations = aclRelations.map { case (name, role, schema, domain) =>
      s"""${formatDotName(
          name.substring(name.indexOf(':') + 1)
        )} -> ${domain}_${schema}_acl_${formatDotName(role)}"""
    }

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

    val rlsRelations = rlsTables.toList.flatMap { case (domainName, tablesMap) =>
      tablesMap.toList.flatMap { case (tableName, rls) =>
        rls.flatMap { r =>
          r.grants.map(userName => (userName, r.name, r.predicate, tableName, domainName))
        }
      }

    }

    val dotRlsRoles = rlsRelations.map { case (userName, name, predicate, schema, domain) =>
      s"""${domain}_${schema}_rls_${formatDotName(name)} [shape=diamond, label = "$predicate"]"""
    }.toSet

    val dotRlsRolesRelations = rlsRelations.map {
      case (userName, name, predicate, schema, domain) =>
        s"${domain}_${schema}_rls_${formatDotName(name)} -> ${domain}_$schema [style=dotted]"
    }.toSet

    val dotRlsRelations = rlsRelations.map { case (userName, name, predicate, schema, domain) =>
      s"""${formatDotName(
          userName.substring(userName.indexOf(':') + 1)
        )} -> ${domain}_${schema}_rls_${formatDotName(name)} [style=dotted]"""
    }

    mkstr(dotRlsRoles.toList, ";\n") +
    mkstr(dotRlsRolesRelations.toList, ";\n") +
    mkstr(dotRlsRelations.toList, ";\n")
  }

  private def aclsAsDot(config: Yml2GraphVizConfig) = {

    val result =
      aclPrefix + tablesAsDot() + usersAsDot() + aclRelationsAsDot() + rlsRelationsAsDot() + suffix

    config.aclOutput match {
      case None => println(result)
      case Some(output) =>
        val file = File(output)
        file.overwrite(result)
    }
  }

  private def asDotRelations(config: Yml2GraphVizConfig): Unit = {
    schemaHandler.domains(config.reload)
    val fkTables = relatedTables().map(_.toLowerCase).toSet
    val dots =
      schemaHandler.domains().map(_.asDot(config.includeAllAttributes.getOrElse(true), fkTables))
    val result = prefix + dots.mkString("\n") + suffix
    config.output match {
      case None => println(result)
      case Some(output) =>
        val file = File(output)
        file.overwrite(result)
    }
  }
}
