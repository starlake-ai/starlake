package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.config.Settings.Connection
import ai.starlake.extract.{ExtractSchema, ExtractSchemaConfig}
import ai.starlake.schema.model.Severity.Error
import ai.starlake.sql.SQLUtils
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.apache.hadoop.fs.Path

case class TaskDesc(version: Int, task: AutoTaskInfo)

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
case class AutoTaskInfo(
  name: String, // Name of the task. Made of jobName + '.' + tableName
  sql: Option[String],
  database: Option[String],
  domain: String,
  table: String,
  presql: List[String] = Nil,
  postsql: List[String] = Nil,
  sink: Option[AllSinks] = None,
  rls: List[RowLevelSecurity] = Nil,
  expectations: List[ExpectationItem] = Nil,
  acl: List[AccessControlEntry] = Nil,
  comment: Option[String] = None,
  freshness: Option[Freshness] = None,
  attributes: List[AutoTaskAttribute] = Nil,
  python: Option[Path] = None,
  tags: Set[String] = Set.empty,
  writeStrategy: Option[WriteStrategy] = None,
  schedule: Option[String] = None,
  dagRef: Option[String] = None,
  _filenamePrefix: String = "", // for internal use. prefix of sql / py file
  parseSQL: Option[Boolean] = None,
  _auditTableName: Option[String] = None,
  taskTimeoutMs: Option[Long] = None,
  _dbComment: Option[String] = None,
  connectionRef: Option[String] = None,
  streams: List[String] = Nil,
  primaryKey: List[String] = Nil
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

  def merge(child: AutoTaskInfo): AutoTaskInfo = {
    AutoTaskInfo(
      name = child.name,
      sql = child.sql,
      database = child.database.orElse(database),
      domain = if (child.domain.isEmpty) domain else child.domain,
      table = if (child.table.isEmpty) table else child.table,
      presql = presql ++ child.presql,
      postsql = postsql ++ child.postsql,
      sink = sink.orElse(child.sink).map(_.merge(child.sink.getOrElse(AllSinks()))),
      rls = rls ++ child.rls,
      expectations = expectations ++ child.expectations,
      acl = acl ++ child.acl,
      comment = child.comment,
      freshness = freshness.orElse(child.freshness),
      attributes = child.attributes,
      python = child.python,
      tags = tags ++ child.tags,
      writeStrategy = child.writeStrategy.orElse(writeStrategy),
      schedule = child.schedule.orElse(schedule),
      dagRef = child.dagRef.orElse(dagRef),
      _filenamePrefix = child._filenamePrefix,
      parseSQL = child.parseSQL.orElse(parseSQL),
      taskTimeoutMs = child.taskTimeoutMs.orElse(taskTimeoutMs),
      connectionRef = child.connectionRef.orElse(connectionRef)
    )
  }
  /*

   */
  def checkValidity()(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    val sinkErrors = sink.map(_.checkValidity(this.name, None)).getOrElse(Right(true))
    val freshnessErrors = freshness.map(_.checkValidity(this.name)).getOrElse(Right(true))
    val emptySchema = new SchemaInfo()
    val writeStrategyErrors = writeStrategy
      .map(
        _.checkValidity(
          this.domain,
          Some(emptySchema.copy(name = this.name, metadata = Some(Metadata(sink = this.sink))))
        )
      )
      .getOrElse(Right(true))

    val scheduleErrors = schedule
      .map { schedule =>
        if (schedule.contains(" ") || settings.appConfig.schedulePresets.contains(schedule)) {
          Right(true)
        } else {
          Left(
            List(
              ValidationMessage(
                Error,
                s"Task $name",
                s"schedule: $schedule is not a valid schedule. Valid schedules are ${settings.appConfig.schedulePresets
                    .mkString(", ")}"
              )
            )
          )
        }
      }
      .getOrElse(Right(true))

    // TODO dag errors

    val dagRefErrors =
      this.dagRef
        .map { dagRef =>
          settings.schemaHandler().checkDagNameValidity(dagRef)
        }
        .getOrElse(Right(true))

    val fullTableName = s"${this.domain}_${this.table}"
    val streamsErrors = streams.map { stream =>
      if (!stream.startsWith(fullTableName)) {
        Left(
          List(
            ValidationMessage(
              Error,
              s"Task $name",
              s"stream: $stream is not a valid stream. Stream should start with $fullTableName"
            )
          )
        )
      } else {
        Right(true)
      }
    }
    // merge all errors
    val allErrors =
      List(sinkErrors, freshnessErrors, writeStrategyErrors, scheduleErrors, dagRefErrors)
    val errorList = allErrors.collect { case Left(errors) => errors }.flatten
    if (errorList.isEmpty)
      Right(true)
    else
      Left(errorList)
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

  def getRunEngine()(implicit settings: Settings): Engine = {
    getRunConnection().getEngine()
  }

  def getSinkConnection()(implicit settings: Settings): Connection = {
    val connection = settings.appConfig
      .connection(getSinkConnectionRef())
      .getOrElse(throw new Exception(s"Connection not found: $connectionRef"))
    connection
  }
  def getSinkConnectionRef()(implicit settings: Settings): String = {
    val connectionRef =
      sink.flatMap { sink => sink.connectionRef }.getOrElse(this.getRunConnectionRef())
    connectionRef
  }

  def getRunConnectionRef()(implicit settings: Settings): String = {
    val transformConnectionRef =
      if (settings.appConfig.transformConnectionRef.isEmpty)
        settings.appConfig.connectionRef
      else
        settings.appConfig.transformConnectionRef
    this.connectionRef.getOrElse(transformConnectionRef)
  }

  def getRunConnection()(implicit settings: Settings): Connection = {
    val connection = settings.appConfig
      .connection(getRunConnectionRef())
      .getOrElse(throw new Exception(s"Connection not found: ${settings.appConfig.connectionRef}"))
    connection
  }

  def getSinkConnectionType()(implicit settings: Settings): ConnectionType = {
    getSinkConnection().`type`
  }

  def getRunConnectionType()(implicit settings: Settings): ConnectionType = {
    getRunConnection().`type`
  }

  def getSinkConfig()(implicit settings: Settings): Sink = {
    val sinkConnectionRef = this.getSinkConnectionRef()
    this.sink
      .map(_.getSink())
      .getOrElse(AllSinks().copy(connectionRef = Some(sinkConnectionRef)).getSink())
  }

  def getTranspiledSql()(implicit settings: Settings): String = {
    val schemaHandler = settings.schemaHandler()
    val allVars = schemaHandler.activeEnvVars()
    val inputSQL = getSql()
    val runConnection = this.getRunConnection()
    schemaHandler.transpileAndSubstitute(
      sql = inputSQL,
      connection = runConnection,
      allVars = allVars,
      test = false
    )
  }

  /** Extracts attributes from the SQL statement.
    * @param settings
    * @return
    *   * List of tuples containing the attribute name and its type. The type is set to "undefined"
    *   as
    */
  def attributesInSqlStatement()(implicit settings: Settings): List[(String, String)] = {
    /*
    val schemaHandler = settings.schemaHandler()
    val sqlWithParametersTranspiled = getTranspiledSql()
    val taskAttributes: List[DiffAttribute] =
      schemaHandler.taskOnly(this.domain + "." + this.table) match {
        case Success(taskInfo) =>
          taskInfo.attributes.map { tAttr => tAttr.toDiffAttribute() }
        case Failure(exception) =>
          // If the task does not exist, we return undefined for all columns
          Nil
      }
    val allTableNames = SQLUtils.extractTableNames(sqlWithParametersTranspiled)

    val dbSchema =
      new DBSchema(
        "",
        this.domain,
        this.table,
        new java.util.ArrayList(taskAttributes.asJava)
      )
    val statementColumns =
      new JSQLSchemaDiff(dbSchema)
        .getDiff(sqlWithParametersTranspiled, s"${this.domain}_${this.table}")
    statementColumns.asScala.toList.map { col =>
      col.getName -> col.getType
    }

     */
    Nil
  }

  /** Extracts attributes from the SQL statement and compares them with the existing attributes in
    * the yaml
    * @param sqlStatementAttributes
    *   List of attributes extracted from an incoming SQL statement
    * @param settings
    * @return
    *   * List of tuples containing the attribute and its status. The status can be one of the
    *   AttributeStatus values:
    *   - ADDED: The attribute is new and not present in the existing attributes
    *   - MODIFIED: The attribute is present but its type or comment has changed
    *   - UNCHANGED: The attribute is present and its type and comment are unchanged
    */
  def diffSqlAttributesWithYaml(
    sqlStatementAttributes: List[AutoTaskAttribute]
  )(implicit settings: Settings): List[(AutoTaskAttribute, AttributeStatus)] = {

    val addedAndModifiedAttributes = sqlStatementAttributes
      .map { sqlAttr =>
        this.attributes.find(_.name.equalsIgnoreCase(sqlAttr.name)) match {
          case Some(existingAttr) =>
            val (updatedType, status) =
              if (existingAttr.`type` == sqlAttr.`type`) {
                existingAttr -> AttributeStatus.UNCHANGED
              } else {
                existingAttr.copy(`type` = sqlAttr.`type`) -> AttributeStatus.MODIFIED
              }
            val (updateTypeAndComment, status2) =
              if (sqlAttr.comment.nonEmpty) {
                updatedType.copy(comment = sqlAttr.comment) -> AttributeStatus.MODIFIED
              } else {
                updatedType -> status
              }
            (updateTypeAndComment, status2)
          case None =>
            sqlAttr -> AttributeStatus.ADDED
        }
      }
    val deletedAttributes = this.attributes
      .filterNot(attDesc => sqlStatementAttributes.exists(_.name.equalsIgnoreCase(attDesc.name)))
      .map(attDesc => attDesc -> AttributeStatus.DELETED)
    addedAndModifiedAttributes ++ deletedAttributes
  }

  def diffSqlAttributesWithYaml()(implicit
    settings: Settings
  ): List[(AutoTaskAttribute, AttributeStatus)] = {
    // Extract attributes from the SQL statement
    val sqlStatementAttributes = this.attributesInSqlStatement().map { case (name, typ) =>
      AutoTaskAttribute(name, typ)
    }
    // Sync attributes with the SQL statement attributes
    this.diffSqlAttributesWithYaml(sqlStatementAttributes)
  }

  /** Update the task description with the new attributes. Existing attributes are updated with the
    * new type and comment. Attributes not present in the new list are dropped.
    *
    * @param incomingAttributes
    *   List of attributes to update the task description with.
    * @param settings
    * @return
    */
  def updateAttributes(
    incomingAttributes: List[AutoTaskAttribute]
  )(implicit settings: Settings): AutoTaskInfo = {
    // Filter out attributes that are not present in the incoming attributes
    val withoutDropped =
      this.attributes.filter { existingAttr =>
        incomingAttributes.exists(_.name.equalsIgnoreCase(existingAttr.name))
      }

    // Update existing attributes with the new type and comment, or keep the existing ones if not
    val addAndUpdatedAttributes = withoutDropped.map { existingAttr =>
      incomingAttributes.find(_.name.equalsIgnoreCase(existingAttr.name)) match {
        case Some(newAttr) => existingAttr.copy(`type` = newAttr.`type`, comment = newAttr.comment)
        case None          => existingAttr
      }
    }
    this.copy(attributes = addAndUpdatedAttributes)
  }

  def diffYamlAttributesWithDB(
    accessToken: Option[String]
  )(implicit settings: Settings): List[(AutoTaskAttribute, AttributeStatus)] = {
    val sql = this.getSql()
    val tableNames = SQLUtils.extractTableNames(sql)
    val schemaHandler = settings.schemaHandler()
    val config = ExtractSchemaConfig(
      tables = tableNames,
      external = true,
      outputDir = None,
      connectionRef = Some(this.getRunConnectionRef()),
      accessToken = accessToken
    )
    val extractor = new ExtractSchema(schemaHandler)
    val dbDomain = extractor.extract(config).getOrElse(Nil).headOption
    val yamlAttributes = this.attributes
    val dbAttributes =
      dbDomain match {
        case Some(domain) =>
          domain.tables.headOption
            .map(_.attributes)
            .getOrElse(Nil)
        case None =>
          Nil
      }
    // list all attributes in the yaml that are not in the db
    val addedAttributes = yamlAttributes
      .filterNot(attr => dbAttributes.exists(_.name.equalsIgnoreCase(attr.name)))
      .map(attr => attr -> AttributeStatus.ADDED)
    // list all attributes in the db that are not in the yaml
    val deletedAttributes = dbAttributes
      .filterNot(attr => yamlAttributes.exists(_.name.equalsIgnoreCase(attr.name)))
      .map(dbAttr =>
        AutoTaskAttribute(
          name = dbAttr.name,
          `type` = dbAttr.`type`,
          comment = dbAttr.comment.getOrElse("")
        ) -> AttributeStatus.DELETED
      )
    // list all attributes in the yaml that are in the db but with a different type or comment
    val modifiedAttributes = yamlAttributes
      .flatMap { yamlAttr =>
        dbAttributes.find(_.name.equalsIgnoreCase(yamlAttr.name)) match {
          case Some(dbAttr)
              if dbAttr.`type` != yamlAttr.`type` || dbAttr.comment.getOrElse(
                ""
              ) != yamlAttr.comment =>
            Some(
              yamlAttr.copy(
                `type` = dbAttr.`type`,
                comment = dbAttr.comment.getOrElse("")
              ) -> AttributeStatus.MODIFIED
            )
          case _ => None
        }
      }
    // return all attributes with their status
    addedAttributes ++ deletedAttributes ++ modifiedAttributes
  }

}

object AutoTaskInfo {
  def compare(existing: AutoTaskInfo, incoming: AutoTaskInfo): ListDiff[Named] = {
    AnyRefDiff.diffAnyRef(existing.name, existing, incoming)
  }
}
