package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.extract.JDBCUtils

import scala.util.Try

case class AccessControlEntry(role: String, grants: Set[String] = Set.empty, name: String = "")
    extends Named {

  def this() = this("", Set.empty) // Should never be called. Here for Jackson deserialization only

  def compare(other: AccessControlEntry): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(this.role, this, other)

  def asHiveSql(tableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal =
        if (grant.startsWith("user:"))
          s"USER ${grant.substring("user:".length)}"
        else if (grant.startsWith("group:") || grant.startsWith("role:"))
          s"ROLE ${grant.substring("group:".length)}"
      s"GRANT ${this.role} ON TABLE $tableName TO $principal"
    }
  }

  def asDatabricksSql(tableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal =
        if (grant.indexOf('@') > 0 && !grant.startsWith("`")) s"`$grant`" else grant
      s"GRANT ${this.role} ON TABLE $tableName TO $principal"
    }
  }

  def asJdbcSql(tableName: String): Set[String] = asHiveSql(tableName)
}

object AccessControlEntry {
  def applyJdbcAcl(connection: Settings.Connection, sqls: Seq[String], forceApply: Boolean = false)(
    implicit settings: Settings
  ): Try[Unit] =
    Try {
      if (forceApply || settings.appConfig.accessPolicies.apply) {
        JDBCUtils.withJDBCConnection(connection.options) { conn =>
          sqls.foreach { sql =>
            val stmt = conn.createStatement()
            stmt.execute(sql)
            stmt.close()
          }
        }
      }
    }

}
