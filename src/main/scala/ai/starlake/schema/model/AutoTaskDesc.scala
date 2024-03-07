package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.config.Settings.Connection
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.apache.hadoop.fs.Path

case class TaskDesc(version: Int, task: AutoTaskDesc)

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
@JsonIgnoreProperties(
  Array("_filenamePrefix", "_auditTableName", "_dbComment", "write")
)
case class AutoTaskDesc(
  name: String,
  sql: Option[String],
  database: Option[String],
  domain: String,
  table: String,
  partition: List[String] = Nil,
  presql: List[String] = Nil,
  postsql: List[String] = Nil,
  sink: Option[AllSinks] = None,
  rls: List[RowLevelSecurity] = Nil,
  expectations: List[ExpectationItem] = Nil,
  acl: List[AccessControlEntry] = Nil,
  comment: Option[String] = None,
  freshness: Option[Freshness] = None,
  attributesDesc: List[AttributeDesc] = Nil,
  python: Option[Path] = None,
  tags: Set[String] = Set.empty,
  writeStrategy: Option[WriteStrategy] = None,
  schedule: Option[String] = None,
  dagRef: Option[String] = None,
  _filenamePrefix: String = "", // for internal use. prefix of sql / py file
  parseSQL: Option[Boolean] = None,
  _auditTableName: Option[String] = None,
  taskTimeoutMs: Option[Long] = None,
  _dbComment: Option[String] = None
) extends Named {

  @JsonIgnore
  def getTableName(): String = this.table

  @JsonIgnore
  def getStrategy()(implicit settings: Settings): WriteStrategy = {
    val st1 = writeStrategy.getOrElse(WriteStrategy(Some(WriteStrategyType.APPEND)))
    val startTs = st1.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = st1.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
    st1.copy(startTs = Some(startTs), endTs = Some(endTs))
  }

  @JsonIgnore
  def getWriteMode(): WriteMode =
    writeStrategy.map(_.toWriteMode()).getOrElse(WriteMode.APPEND)

  def merge(child: AutoTaskDesc): AutoTaskDesc = {
    AutoTaskDesc(
      name = child.name,
      sql = child.sql,
      database = child.database.orElse(database),
      domain = if (child.domain.isEmpty) domain else child.domain,
      table = if (child.table.isEmpty) table else child.table,
      partition = if (child.partition.isEmpty) partition else child.partition,
      presql = presql ++ child.presql,
      postsql = postsql ++ child.postsql,
      sink = sink.orElse(child.sink).map(_.merge(child.sink.getOrElse(AllSinks()))),
      rls = rls ++ child.rls,
      expectations = expectations ++ child.expectations,
      acl = acl ++ child.acl,
      comment = child.comment,
      freshness = freshness.orElse(child.freshness),
      attributesDesc = child.attributesDesc,
      python = child.python,
      tags = tags ++ child.tags,
      writeStrategy = child.writeStrategy.orElse(writeStrategy),
      schedule = child.schedule.orElse(schedule),
      dagRef = child.dagRef.orElse(dagRef),
      _filenamePrefix = child._filenamePrefix,
      parseSQL = child.parseSQL.orElse(parseSQL),
      taskTimeoutMs = child.taskTimeoutMs.orElse(taskTimeoutMs)
    )
  }

  def checkValidity(): Either[List[String], Boolean] = {
    freshness.map(_.checkValidity()).getOrElse(Right(true)).left.map { errors =>
      errors.map { error =>
        s"freshness: $error"
      }
    }
    Right(true)
  }

  def this() = this(
    name = "",
    sql = None,
    database = None,
    domain = "",
    table = "",
    python = None,
    writeStrategy = None,
    taskTimeoutMs = None
  ) // Should never be called. Here for Jackson deserialization only

  def getSql(): String = sql.getOrElse("")

  def getDatabase()(implicit settings: Settings): Option[String] = {
    database
      .orElse(settings.appConfig.getDefaultDatabase()) // database passed in env vars
  }

  def getEngine()(implicit settings: Settings): Engine = {
    getDefaultConnection().getEngine()
  }

  def getSinkConnection()(implicit settings: Settings): Connection = {
    val connectionRef =
      sink.flatMap { sink => sink.connectionRef }.getOrElse(settings.appConfig.connectionRef)
    val connection = settings.appConfig
      .connection(connectionRef)
      .getOrElse(throw new Exception(s"Connection not found: $connectionRef"))
    connection
  }

  def getDefaultConnection()(implicit settings: Settings): Connection = {
    val connection = settings.appConfig
      .connection(settings.appConfig.connectionRef)
      .getOrElse(throw new Exception(s"Connection not found: ${settings.appConfig.connectionRef}"))
    connection
  }

  def getSinkConnectionType()(implicit settings: Settings): ConnectionType = {
    getSinkConnection().getType()
  }

  def getSinkConfig()(implicit settings: Settings): Sink =
    this.sink.map(_.getSink()).getOrElse(AllSinks().getSink())
}

object AutoTaskDesc {
  def compare(existing: AutoTaskDesc, incoming: AutoTaskDesc): ListDiff[Named] = {
    AnyRefDiff.diffAnyRef(existing.name, existing, incoming)
  }
}
