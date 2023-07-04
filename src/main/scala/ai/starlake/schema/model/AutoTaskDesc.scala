package ai.starlake.schema.model

import ai.starlake.config.{DatasetArea, Settings, StorageArea}
import ai.starlake.schema.handlers.SchemaHandler
import org.apache.hadoop.fs.Path

/** Task executed in the context of a job. Each task is executed in its own session.
  *
  * @param sql
  *   Main SQL request to exexute (do not forget to prefix table names with the database name to
  *   avoid conflicts)
  * @param domain
  *   Output domain in output Area (Will be the Database name in Hive or Dataset in BigQuery)
  * @param table
  *   Dataset Name in output Area (Will be the Table name in Hive & BigQuery)
  * @param write
  *   Append to or overwrite existing data
  * @param partition
  *   List of columns used for partitioning the outtput.
  * @param presql
  *   List of SQL requests to executed before the main SQL request is run
  * @param postsql
  *   List of SQL requests to executed after the main SQL request is run
  * @param sink
  *   Where to sink the data
  * @param rls
  *   Row level security policy to apply too the output data.
  */
case class AutoTaskDesc(
  name: String,
  sql: Option[String],
  database: Option[String],
  domain: String,
  table: String,
  write: WriteMode,
  partition: List[String] = Nil,
  presql: List[String] = Nil,
  postsql: List[String] = Nil,
  sink: Option[Sink] = None,
  rls: List[RowLevelSecurity] = Nil,
  expectations: Map[String, String] = Map.empty,
  engine: Option[Engine] = None,
  acl: List[AccessControlEntry] = Nil,
  comment: Option[String] = None,
  format: Option[String] = None,
  coalesce: Option[Boolean] = None,
  freshness: Option[Freshness] = None,
  attributesDesc: List[AttributeDesc] = Nil,
  python: Option[Path] = None,
  tags: Set[String] = Set.empty,
  merge: Option[MergeOptions]
) extends Named {

  def checkValidity(): Either[List[String], Boolean] = {
    freshness.map(_.checkValidity()).getOrElse(Right(true)).left.map { errors =>
      errors.map { error =>
        s"freshness: $error"
      }
    }
  }

  def this() = this(
    name = "",
    sql = None,
    database = None,
    domain = "",
    table = "",
    write = WriteMode.OVERWRITE,
    python = None,
    merge = None
  ) // Should never be called. Here for Jackson deserialization only

  def getSql(): String = sql.getOrElse("")

  /** Return a Path only if a storage area s defined
    * @return
    */
  def getTargetPath()(implicit settings: Settings): Path = {
    new Path(DatasetArea.path(domain), table)
  }

  def getHiveDB()(implicit settings: Settings): String = {
    StorageArea.area(domain, None)
  }

  def getDatabase(implicit
    settings: Settings,
    schemaHandler: SchemaHandler
  ): Option[String] = {
    database
      .orElse(
        schemaHandler.activeEnvRefs().getDatabase(domain, table)
      ) // mapping in envRefs
      .orElse(settings.comet.getDatabase()) // database passed in env vars
  }

}

object AutoTaskDesc {
  def compare(existing: AutoTaskDesc, incoming: AutoTaskDesc): ListDiff[Named] = {
    AnyRefDiff.diffAnyRef(existing.name, existing, incoming)
  }
}
