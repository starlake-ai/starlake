package ai.starlake.schema.model

import ai.starlake.config.Settings.Connection
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.ExtractSchema
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.schema.CaseInsensitiveLinkedHashMap
import ai.starlake.transpiler.JSQLSchemaDiff
import ai.starlake.transpiler.diff
import ai.starlake.transpiler.diff.{Attribute => DiffAttribute, DBSchema}
import ai.starlake.utils.SparkUtils
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

case class Task(task: AutoTaskDesc)

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
  attributes: List[Attribute] = Nil,
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
  _dbComment: Option[String] = None,
  connectionRef: Option[String] = None,
  metadata: Option[Metadata] = None,
  syncStrategy: Option[TableSync] = None
) extends Named {

  def fullName() = s"${this.domain}.${this.name}"

  @JsonIgnore
  def getTableName(): String = this.table

  @JsonIgnore
  def getStrategy()(implicit settings: Settings): WriteStrategy = {
    val st1 = writeStrategy.getOrElse(WriteStrategy(Some(WriteStrategyType.APPEND)))
    val startTs = st1.start_ts.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = st1.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp)
    st1.copy(start_ts = Some(startTs), end_ts = Some(endTs))
  }

  @JsonIgnore
  def getWriteMode(): WriteMode = write.getOrElse(WriteMode.OVERWRITE)

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
    this.connectionRef.getOrElse(settings.appConfig.connectionRef)
  }

  def getRunConnection()(implicit settings: Settings): Connection = {
    val connection = settings.appConfig
      .connection(getRunConnectionRef())
      .getOrElse(throw new Exception(s"Connection not found: ${settings.appConfig.connectionRef}"))
    connection
  }

  def getSinkConnectionType()(implicit settings: Settings): ConnectionType = {
    getSinkConnection().getType()
  }

  def getRunConnectionType()(implicit settings: Settings): ConnectionType = {
    getRunConnection().getType()
  }

  def getSinkConfig()(implicit settings: Settings): Sink =
    this.sink.map(_.getSink()).getOrElse(AllSinks().getSink())

  def getTranspiledSql(
    extraVars: Map[String, String] = Map.empty
  )(implicit settings: Settings): String = {
    val schemaHandler = settings.schemaHandler()
    val allVars = schemaHandler.activeEnvVars() ++ extraVars
    val inputSQL = getSql()
    val runConnection = this.getRunConnection()
    if (parseSQL.getOrElse(true))
      schemaHandler.transpileAndSubstituteSelectStatement(
        sql = inputSQL,
        connection = runConnection,
        allVars = allVars,
        test = false
      )
    else
      inputSQL
  }

  /** Extracts attributes from the SQL statement.
    * @param settings
    * @return
    *   * List of tuples containing the attribute name and its type. The type is set to "undefined"
    *   as
    */
  def attributesInSqlStatement(
    sqlStatement: Option[String]
  )(implicit settings: Settings): List[(String, String)] = {
    val schemaHandler = settings.schemaHandler()
    val sqlWithParametersTranspiled = sqlStatement.getOrElse(getTranspiledSql())

    val allTableNames = SQLUtils.extractTableNames(sqlWithParametersTranspiled)
    val thisTableName = s"${this.domain}.${this.table}"
    val tablesGroupedByDomain: List[(String, List[String])] =
      (thisTableName +: allTableNames).distinct
        .flatMap { fullTableName =>
          val components = fullTableName.split('.')
          assert(
            components.length >= 2,
            s"Table name $fullTableName should be composed of domain and table name separated by a dot"
          )
          val domainName = components(components.length - 2)
          val schemaName = components(components.length - 1)
          schemaHandler
            .taskByTableName(domainName, schemaName)
            .filter(it => it.attributes.nonEmpty)
            .map { t =>
              logger.info(
                s"Found task ${t.name} for table $fullTableName with attributes ${t.attributes.map(_.name).mkString(", ")}"
              )
              (t.domain, t.table)
            }
            .orElse {
              schemaHandler.tableByFinalName(domainName, schemaName).map { t =>
                logger.info(
                  s"Found table ${t.name} for table $fullTableName with attributes ${t.attributes.map(_.name).mkString(", ")}"
                )
                (domainName, schemaName)
              }
            }
            .orElse {
              schemaHandler.external(domainName, schemaName).map { t =>
                logger.info(
                  s"Found external table ${t.name} for table $fullTableName with attributes ${t.attributes.map(_.name).mkString(", ")}"
                )
                (domainName, schemaName)
              }
            }
            .orElse {
              new ExtractSchema(schemaHandler)
                .extractTable(fullTableName, None)
                .toOption
                .filter(_.tables.nonEmpty)
                .map { it =>
                  logger.info(
                    s"Found extracted table ${it.tables.head.name} for table $fullTableName with attributes ${it.tables.head.attributes.map(_.name).mkString(", ")}"
                  )
                  (it.finalName, it.tables.head.finalName)
                }
            }
        }
        .groupBy { case (domain, _) =>
          domain
        }
        .mapValues(_.map(_._2).distinct)
        .toList

    val dbSchemas =
      tablesGroupedByDomain.map { case (domain, tables) =>
        val tablesMap = new CaseInsensitiveLinkedHashMap[java.util.Collection[DiffAttribute]]()
        tables.map { table =>
          val attrs = schemaHandler
            .attributesAsDiff(domain, table)
          tablesMap.put(table, attrs.asJava)
        }
        new DBSchema("", domain, tablesMap)
      }.asJava

    // logger.info(YamlSerializer.serialize(dbSchemas)) // FIXME ?
    logger.info(sqlWithParametersTranspiled)

    val statementColumns =
      new JSQLSchemaDiff(dbSchemas)
        .getDiff(sqlWithParametersTranspiled, s"${this.domain}.${this.table}")

    val result =
      statementColumns.asScala.toList.filter(_.getStatus != diff.AttributeStatus.REMOVED).map {
        col =>
          col.getName -> col.getType
      }

    result
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
    sqlStatementAttributes: List[Attribute]
  )(implicit settings: Settings): List[(Attribute, AttributeStatus)] = {

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
      .map(attDesc => attDesc -> AttributeStatus.REMOVED)
    addedAndModifiedAttributes ++ deletedAttributes
  }

  def diffSqlAttributesWithYaml(
    sqlStatement: Option[String] = None
  )(implicit
    settings: Settings
  ): List[(Attribute, AttributeStatus)] = {
    if (parseSQL.getOrElse(true)) {
      // Extract attributes from the SQL statement
      val sqlStatementAttributes =
        this.attributesInSqlStatement(sqlStatement).map { case (name, typ) =>
          Attribute(name, typ)
        }
      // Sync attributes with the SQL statement attributes
      this.diffSqlAttributesWithYaml(sqlStatementAttributes)
    } else {
      logger.info(
        s"Skipping diff Sql Attributes With Yaml for task ${this.name} as parseSQL is set to false"
      )
      Nil
    }
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
    incomingAttributes: List[Attribute]
  )(implicit settings: Settings): AutoTaskDesc = {
    // Filter out attributes that are not present in the incoming attributes
    val withoutDropped =
      this.attributes.filter { existingAttr =>
        incomingAttributes.exists(_.name.equalsIgnoreCase(existingAttr.name))
      }

    // Update existing attributes with the new type and comment, or keep the existing ones if not
    val addAndUpdatedAttributes = incomingAttributes.map { newAttr =>
      this.attributes.find(_.name.equalsIgnoreCase(newAttr.name)) match {
        case Some(existingAttr) =>
          existingAttr.copy(`type` = newAttr.`type`, comment = newAttr.comment)
        case None => newAttr
      }
    }
    this.copy(attributes = addAndUpdatedAttributes)
  }

  @JsonIgnore
  def getSyncStrategyValue(): TableSync = syncStrategy.getOrElse(TableSync.ADD)

  def readyForSync(): Boolean = {
    this.attributes.nonEmpty &&
    this.attributes.forall(_.`type`.nonEmpty) &&
    this.getSyncStrategyValue() != TableSync.NONE
  }

  def sparkSchema(
    schemaHandler: SchemaHandler
  ): StructType = {
    val temporary = this.name.startsWith("zztmp_")
    SparkUtils.sparkSchemaWithCondition(
      schemaHandler = schemaHandler,
      attributes = attributes,
      p = _ => true,
      withFinalName = !temporary
    )
  }

}

object AutoTaskDesc {
  def compare(existing: AutoTaskDesc, incoming: AutoTaskDesc): ListDiff[Named] = {
    AnyRefDiff.diffAnyRef(existing.name, existing, incoming)
  }
}
