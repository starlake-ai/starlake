package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.utils.Utils

import scala.util.Try

case class AccessControlEntry(role: String, grants: Set[String] = Set.empty, name: String = "")
    extends SecurityLevel {

  override def toString: String = {
    s"AccessControlEntry(role=$role, grants=$grants)"
  }

  def asMap(): Map[String, String] = Map(
    "aceRole"   -> role,
    "aceGrants" -> grants.mkString(",")
  )

  def this() = this("", Set.empty) // Should never be called. Here for Jackson deserialization only

  def compare(other: AccessControlEntry): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(this.role, this, other)

  private def asHiveSql(fullTableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal =
        if (grant.startsWith("user:"))
          s"USER ${grant.substring("user:".length)}"
        else if (grant.startsWith("group:") || grant.startsWith("role:"))
          s"ROLE ${grant.substring("group:".length)}"
      s"GRANT ${this.role} ON TABLE $fullTableName TO $principal"
    }
  }

  private def asDatabricksSql(fullTableName: String): Set[String] = {
    /*
  GRANT
    privilege_type [, privilege_type ] ...
    ON (CATALOG | DATABASE <database-name> | TABLE <table-name> | VIEW <view-name> | FUNCTION <function-name> | ANONYMOUS FUNCTION | ANY FILE)
    TO principal

  privilege_type
    : SELECT | CREATE | MODIFY | READ_METADATA | CREATE_NAMED_FUNCTION | ALL PRIVILEGES
     */
    this.grants.map { grant =>
      val principal =
        if (grant.indexOf('@') > 0 && !grant.startsWith("`")) s"`$grant`" else grant
      s"GRANT ${this.role} ON TABLE $fullTableName TO $principal"
    }
  }

  private def asSnowflakeSql(fullTableName: String): Set[String] = {
    /*
      https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
      https://hevodata.com/learn/snowflake-grant-role-to-user/
     */
    this.grants.map { principal =>
      s"GRANT ${this.role} ON TABLE $fullTableName TO DATABASE ROLE $principal"
    }
  }

  def asBigQuerySql(fullTableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal =
        if (!grant.startsWith("\""))
          s""""$grant""""
        else
          grant
      s"GRANT ${this.role} ON TABLE $fullTableName TO $principal"
    }
  }

  def asSql(fullTableName: String, engine: Engine): Set[String] = engine match {
    case Engine.SPARK =>
      if (Utils.isRunningInDatabricks())
        asDatabricksSql(fullTableName)
      else
        asHiveSql(fullTableName)
    case Engine.BQ => asBigQuerySql(fullTableName)
    case e =>
      if (e.toString() == "snowflake")
        asSnowflakeSql(fullTableName)
      else
        asJdbcSql(fullTableName)
  }

  private def asJdbcSql(fullTableName: String): Set[String] = {
    asHiveSql(fullTableName)
  }
}

object AccessControlEntry {
  def applyJdbcAcl(connection: Settings.ConnectionInfo, sqls: Seq[String], forceApply: Boolean)(
    implicit settings: Settings
  ): Try[Unit] =
    Try {
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), connection.options) {
        conn =>
          applyJdbcAcl(conn, sqls, forceApply).get
      }
    }

  def applyJdbcAcl(
    jdbcConnection: java.sql.Connection,
    sqls: Seq[String],
    forceApply: Boolean
  )(implicit
    settings: Settings
  ): Try[Unit] =
    Try {
      if (forceApply || settings.appConfig.accessPolicies.apply)
        sqls.foreach { sql =>
          JdbcDbUtils.executeUpdate(sql, jdbcConnection)
        }
    }

}
