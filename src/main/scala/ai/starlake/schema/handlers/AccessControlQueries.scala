package ai.starlake.schema.handlers

import ai.starlake.schema.model._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

object AccessControlQueries {

  def acls(
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo]
  ): List[(String, List[AccessControlEntry])] = {
    domains.flatMap { domain =>
      domain.tables.map { table =>
        (domain.finalName + "." + table.finalName, table.acl)
      }
    } ++
    tasks.map { task =>
      (task.domain + "." + task.name, task.acl)
    }
  }

  def rls(
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo]
  ): List[(String, List[RowLevelSecurity])] = {
    domains.flatMap { domain =>
      domain.tables.map { table =>
        (domain.finalName + "." + table.finalName, table.rls)
      }
    } ++
    tasks.map { task =>
      (task.domain + "." + task.name, task.rls)
    }
  }

  def cls(iamPolicyTags: Option[IamPolicyTags]): List[IamPolicyTag] =
    iamPolicyTags.toList.flatMap { _.iamPolicyTags }

  def accessPolicies(
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo]
  ): CaseInsensitiveMap[String] = {
    val result =
      domains.flatMap { domain =>
        domain.tables.flatMap { table =>
          table.attributes.flatMap { attr =>
            attr.accessPolicy.map { policy =>
              (s"${domain.finalName}.${table.finalName}.${attr.getFinalName()}", policy)
            }
          }
        }
      }.toMap ++
      tasks.flatMap { task =>
        task.attributes.flatMap { attr =>
          attr.accessPolicy.map { policy =>
            (s"${task.domain}.${task.name}.${attr.getFinalName()}", policy)
          }
        }
      }.toMap
    CaseInsensitiveMap(result)
  }
}
