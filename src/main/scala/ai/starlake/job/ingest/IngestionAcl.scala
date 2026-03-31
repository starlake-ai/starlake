package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.schema.model._
import ai.starlake.utils.SparkUtils

import scala.util.Try

/** ACL management concerns for ingestion jobs. */
trait IngestionAcl { self: IngestionJob =>

  private def extractHiveTableAcl(): List[String] = {
    if (settings.appConfig.isHiveCompatible()) {
      val fullTableName = schemaHandler.getFullTableName(domain, schema)
      schema.acl.flatMap { ace =>
        ace.asSql(fullTableName, engine = Engine.SPARK)
      }
    } else {
      Nil
    }
  }

  def applyHiveTableAcl(): Try[Unit] =
    Try {
      val sqls = extractHiveTableAcl()
      sqls.foreach { sql =>
        SparkUtils.sql(session, sql)
      }
    }

  def applyJdbcAcl(connection: Settings.ConnectionInfo, forceApply: Boolean = false): Try[Unit] = {
    val fullTableName = schemaHandler.getFullTableName(domain, schema)
    val sqls =
      schema.acl.flatMap { ace =>
        ace.asSql(fullTableName, connection.getJdbcEngineName())
      }
    AccessControlEntry.applyJdbcAcl(
      connection,
      sqls,
      forceApply
    )
  }
}
