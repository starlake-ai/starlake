package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.utils.Utils

import scala.util.Try

case class AccessControlEntry(role: String, grants: Set[String] = Set.empty, name: String = "")
    extends Named {

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

  def asSnowflakeSql(tableName: String): Set[String] = {
    this.grants.map { principal =>
      s"GRANT ${this.role} ON TABLE $tableName TO DATABASE ROLE $principal"
    }
  }

  def asBigQuerySql(tableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal =
        if (!grant.startsWith("\""))
          s""""$grant""""
        else
          grant
      s"GRANT ${this.role} ON TABLE $tableName TO $principal"
    }
  }

  def asSql(tableName: String, engine: Engine): Set[String] = engine match {
    case Engine.SPARK =>
      if (Utils.isRunningInDatabricks())
        asDatabricksSql(tableName)
      else
        asHiveSql(tableName)
    case Engine.BQ => asBigQuerySql(tableName)
    case e =>
      if (e.toString() == "snowflake")
        asSnowflakeSql(tableName)
      else
        asJdbcSql(tableName)
  }

  def asJdbcSql(tableName: String): Set[String] = {
    /*
      https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
      https://hevodata.com/learn/snowflake-grant-role-to-user/
     */
    asHiveSql(tableName)
  }
}

object AccessControlEntry {
  def applyJdbcAcl(connection: Settings.Connection, sqls: Seq[String], forceApply: Boolean)(implicit
    settings: Settings
  ): Try[Unit] =
    Try {
      JdbcDbUtils.withJDBCConnection(connection.options) { conn =>
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
