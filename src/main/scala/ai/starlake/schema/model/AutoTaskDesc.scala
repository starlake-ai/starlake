package ai.starlake.schema.model

import ai.starlake.config.Settings.Connection
import ai.starlake.config.{DatasetArea, Settings}
import com.fasterxml.jackson.annotation.JsonIgnore
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
  write: Option[WriteMode],
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
  recursive: Boolean = false,
  _filenamePrefix: String = "", // for internal use. prefix of sql / py file
  parseSQL: Option[Boolean] = None,
  _auditTableName: Option[String] = None,
  taskTimeoutMs: Option[Long] = None,
  _dbComment: Option[String] = None
) extends Named {

  @JsonIgnore
  def getTableName(): String = this.table

  def getStrategy()(implicit settings: Settings): WriteStrategy = {
    val st1 = writeStrategy
      .getOrElse(WriteStrategy(WriteStrategyType.APPEND))

    val startTs = st1.start_ts.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = st1.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp)
    st1.copy(start_ts = Some(startTs), end_ts = Some(endTs))
  }

  def getWrite(): WriteMode = write.getOrElse(WriteMode.OVERWRITE)

  def merge(child: AutoTaskDesc): AutoTaskDesc = {
    AutoTaskDesc(
      name = child.name,
      sql = child.sql,
      database = child.database.orElse(database),
      domain = if (child.domain.isEmpty) domain else child.domain,
      table = if (child.table.isEmpty) table else child.table,
      write = child.write.orElse(write),
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
    if (writeStrategy.isDefined && write.isDefined && write.get != WriteMode.OVERWRITE) {
      Left(List("Merge and write mode are not compatible"))
    } else {
      Right(true)
    }
  }

  def this() = this(
    name = "",
    sql = None,
    database = None,
    domain = "",
    table = "",
    write = Some(WriteMode.OVERWRITE),
    python = None,
    writeStrategy = None,
    taskTimeoutMs = None
  ) // Should never be called. Here for Jackson deserialization only

  def getSql(): String = sql.getOrElse("")

  /** Return a Path only if a storage area s defined
    * @return
    */
  @JsonIgnore
  def getTargetPath()(implicit settings: Settings): Path = {
    val auditDomain = settings.appConfig.audit.getDomain()
    if (domain == auditDomain) {
      table match {
        case "continuous" | "discrete" | "frequencies" =>
          DatasetArea.metrics(domain, table)
        case "audit" | "expectations" =>
          DatasetArea.audit(domain, table)
        case "rejected" =>
          new Path(DatasetArea.rejected(domain), table)
        case _ =>
          throw new Exception(s"$table: Audit table name not supported")
      }
    } else
      new Path(DatasetArea.business(domain), table)
  }

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
