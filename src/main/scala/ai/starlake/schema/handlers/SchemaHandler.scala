/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.schema.handlers

import ai.starlake.config.Settings.{latestSchemaVersion, AppConfig}
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.ingest.{AuditLog, RejectedRecord}
import ai.starlake.job.metrics.ExpectationReport
import ai.starlake.schema.model.Severity._
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter._
import ai.starlake.utils.{StarlakeObjectMapper, Utils, YamlSerde}
import better.files.{File, Resource}
import com.databricks.spark.xml.util.XSDToSchema
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class TableWithNameOnly(name: String, attrs: List[String])

case class DomainWithNameOnly(name: String, tables: List[TableWithNameOnly])

object SchemaHandler {
  private val watcherMap: scala.collection.concurrent.TrieMap[String, Settings] =
    scala.collection.concurrent.TrieMap.empty

}

/** Handles access to datasets metadata, eq. domains / types / schemas.
  *
  * @param storage
  *   : Underlying filesystem manager
  */
class SchemaHandler(storage: StorageHandler, cliEnv: Map[String, String] = Map.empty)(implicit
  settings: Settings
) extends StrictLogging {
  /*
  if (settings.appConfig.fileSystem.startsWith("file:")) {
    val handler = new MetadataFileChangeHandler(this)
    val watcher = new RecursiveFileMonitor(File(DatasetArea.metadata.toString)) {
      override def onEvent(
        eventType: WatchEvent.Kind[java.nio.file.Path],
        file: File,
        count: Int
      ): Unit = {
        handler.onEvent(eventType, file, count)
      }
    }
    import scala.concurrent.ExecutionContext.Implicits.global
    watcher.start()
  } else {
    logger.warn(
      "File system is not local, file watcher is not available. Please use local file system for file watcher."
    )
  }
   */
  private val forceJobPrefixRegex: Regex = settings.appConfig.forceJobPattern.r
  private val forceTaskPrefixRegex: Regex = settings.appConfig.forceTablePattern.r

  // uses Jackson YAML for parsing, relies on SnakeYAML for low level handling
  @nowarn val mapper: ObjectMapper with ScalaObjectMapper =
    new StarlakeObjectMapper(new YAMLFactory(), injectables = (classOf[Settings], settings) :: Nil)

  def listen(): Unit = {
    MetadataFileChangeHandler.start(this)
  }
  listen()
  @throws[Exception]
  private def checkTypeDomainsJobsValidity(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    reload: Boolean = false
  )(implicit storage: StorageHandler): List[ValidationMessage] = {

    val domainStructureValidity = Domain.checkFilenamesValidity()(storage, settings)
    val types = this.types(reload)
    if (types.isEmpty) {
      throw new Exception(
        "No types defined. Please define types in metadata/types/default.sl.yml"
      )
    }

    val typesValidity = types.map(_.checkValidity())
    val loadedDomains = this.domains(domainNames, tableNames, reload)
    val loadedDomainsValidity = loadedDomains.map(_.checkValidity(this))
    val domainsValidity =
      domainStructureValidity ++ loadedDomainsValidity
    val domainsVarsValidity = checkDomainsVars()
    val jobsVarsValidity = checkJobsVars() // job vars may be defined at runtime.
    val allErrors = typesValidity ++ domainsValidity

    val errs = allErrors.flatMap {
      case Left(values) => values
      case Right(_)     => Nil
    }
    errs ++ domainsVarsValidity ++ jobsVarsValidity
  }

  private def checkDomainsVars(): List[ValidationMessage] = {
    storage
      .list(
        DatasetArea.load,
        extension = ".sl.yml",
        recursive = true,
        exclude = Some(Pattern.compile("_.*"))
      )
      .map(_.path)
      .flatMap(checkVarsAreDefined)
  }

  def checkJobsVars(): List[ValidationMessage] = {
    val paths =
      storage.list(DatasetArea.transform, ".sl.yml", recursive = true).map(_.path)
    val ymlWarnings = paths.flatMap(checkVarsAreDefined)
    val sqlPaths =
      (storage.list(DatasetArea.transform, ".sql.j2", recursive = true) ++ storage.list(
        DatasetArea.transform,
        ".sql",
        recursive = true
      ) ++ storage.list(
        DatasetArea.transform,
        ".py",
        recursive = true
      )).map(_.path)
    val sqlWarnings = sqlPaths.flatMap { path =>
      val filename = path.getName()
      val prefix = if (filename.endsWith(".sql.j2")) {
        filename.dropRight(7)
      } else if (filename.endsWith(".sql")) {
        filename.dropRight(4)
      } else { // .py
        filename.dropRight(3)
      }
      val sqlPath = taskCommandPath(path.getParent(), prefix)
      sqlPath.map(checkVarsAreDefined).getOrElse(Nil)
    }

    ymlWarnings ++ sqlWarnings
  }

  def checkValidity(
    config: ValidateConfig = ValidateConfig()
  ): Try[(List[ValidationMessage], Int, Int)] = Try {
    val envErrorsAndWarnings = EnvDesc.checkValidity(storage, settings)
    val settingsErrorsAndWarnings = AppConfig.checkValidity(storage, settings)
    val typesDomainsJobsErrorsAndWarnings =
      checkTypeDomainsJobsValidity(reload = config.reload)(storage)
    val deserErrors = deserializedDomains(DatasetArea.load)
      .filter { case (path, res) =>
        res.isFailure
      } map { case (path, err) =>
      ValidationMessage(
        severity = Error,
        target = path.toString,
        message = s"${path.toString} could not be deserialized"
      )
    }

    val allErrorsAndWarnings =
      envErrorsAndWarnings ++ settingsErrorsAndWarnings ++ typesDomainsJobsErrorsAndWarnings ++ deserErrors ++ this._domainErrors ++ this._jobErrors
    val (warnings, errors) = allErrorsAndWarnings.partition(_.severity == Warning)
    val errorCount = errors.length
    val warningCount = warnings.length

    (allErrorsAndWarnings, errorCount, warningCount)
  }

  def getDdlMapping(schema: Schema): Map[String, Map[String, String]] = {

    schema.attributes.flatMap { attr =>
      val ddlMapping = types().find(_.name == attr.`type`).map(_.ddlMapping)
      ddlMapping match {
        case Some(Some(mapping)) =>
          Some(attr.name -> mapping) // we found the primitive type and it has a ddlMapping
        case None       => None // we did not find the primitive type (should never happen)
        case Some(None) => None // we found the primitive type but it has no ddlMapping
      }
    }.toMap
  }

  def getAttributesWithDDLType(schema: Schema, dbName: String): Map[String, String] = {
    schema.attributes.flatMap { attr =>
      val ddlMapping = types().find(_.name == attr.`type`).map(_.ddlMapping)
      ddlMapping match {
        case Some(Some(mapping)) =>
          Some(
            attr.name -> mapping.getOrElse(
              dbName,
              throw new Exception(
                s"${attr.name}: ${attr.`type`} DDL mapping not found for $dbName"
              )
            )
          ) // we found the primitive type and it has a ddlMapping
        case None =>
          throw new Exception(s"${attr.name}: ${attr.`type`} DDL mapping not found")
          None // we did not find the primitive type (should never happen)
        case Some(None) =>
          throw new Exception(s"${attr.name}: ${attr.`type`} DDL mapping not found (None)")
          None // we found the primitive type but it has no ddlMapping
      }
    }.toMap
  }

  def loadTypes(filename: String): List[Type] = {
    // TODO: remove deprecated file path in a version
    val deprecatedTypesPath = new Path(DatasetArea.types, filename + ".yml")
    val typesCometPath = new Path(DatasetArea.types, filename + ".sl.yml")
    if (storage.exists(typesCometPath)) {
      YamlSerde.deserializeYamlTypes(storage.read(typesCometPath), typesCometPath.toString)
    } else if (storage.exists(deprecatedTypesPath)) {
      YamlSerde.deserializeYamlTypes(
        storage.read(deprecatedTypesPath),
        typesCometPath.toString
      )
    } else
      List.empty[Type]
  }

  def types(reload: Boolean = false): List[Type] = if (reload) loadTypes() else _types
  var _types: List[Type] = loadTypes()

  /** All defined types. Load all default types defined in the file default.sl.yml Types are located
    * in the only file "types.sl.yml" Types redefined in the file "types.sl.yml" supersede the ones
    * in "default.sl.yml"
    */
  @throws[Exception]
  private def loadTypes(): List[Type] = {
    val resourceTypes = Resource.asString("types/default.sl.yml") match {
      case Some(value) =>
        YamlSerde.deserializeYamlTypes(value, "resource:types/default.sl.yml")
      case None =>
        Nil
    }
    val defaultTypes = loadTypes("default") :+ Type("struct", ".*", PrimitiveType.struct)
    val types = loadTypes("types")

    val redefinedResourceTypeNames = resourceTypes.map(_.name).intersect(defaultTypes.map(_.name))
    val defaultAndRedefinedTypes =
      resourceTypes.filter(resourceType =>
        !redefinedResourceTypeNames.contains(resourceType.name)
      ) ++ defaultTypes

    val redefinedTypeNames = defaultAndRedefinedTypes.map(_.name).intersect(types.map(_.name))

    this._types = defaultAndRedefinedTypes.filter(defaultType =>
      !redefinedTypeNames.contains(defaultType.name)
    ) ++ types

    this._types
  }

  lazy val jinjavaMacros: String = {
    val j2Files = storage.list(DatasetArea.expectations, ".j2", recursive = true).map(_.path)
    val macros = j2Files
      .map { path =>
        val content = storage.read(path)
        "\n" + content + "\n"
      }
    macros.mkString("\n")
  }

  private def viewName(sqlFile: Path) =
    if (sqlFile.getName().endsWith(".sql.j2"))
      sqlFile.getName().dropRight(".sql.j2".length)
    else
      sqlFile.getName().dropRight(".sql".length)

  private def loadSqlJ2File(sqlFile: Path): (String, String) = {
    val sqlExpr = storage.read(sqlFile)
    val sqlName = viewName(sqlFile)
    sqlName -> sqlExpr
  }

  private def loadSqlJ2File(path: Path, viewName: String): Option[String] = {
    val viewFile =
      listSqlj2Files(path).find(_.path.getName().startsWith(s"$viewName.sql")).map(_.path)
    viewFile.map { viewFile =>
      val (viewName, viewContent) = loadSqlJ2File(viewFile)
      viewContent
    }
  }

  private def listSqlj2Files(path: Path): List[FileInfo] = {
    val j2Files = storage.list(path, extension = ".sql.j2", recursive = true)
    val sqlFiles = storage.list(path, extension = ".sql", recursive = true)
    val allFiles = j2Files ++ sqlFiles
    allFiles
  }

  val slDateVars: Map[String, String] = {
    val today = LocalDateTime.now
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val yearFormatter = DateTimeFormatter.ofPattern("yyyy")
    val monthFormatter = DateTimeFormatter.ofPattern("MM")
    val dayFormatter = DateTimeFormatter.ofPattern("dd")
    val hourFormatter = DateTimeFormatter.ofPattern("HH")
    val minuteFormatter = DateTimeFormatter.ofPattern("mm")
    val secondFormatter = DateTimeFormatter.ofPattern("ss")
    val milliFormatter = DateTimeFormatter.ofPattern("SSS")
    val epochMillis = today.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli
    Map(
      "sl_date"         -> today.format(dateFormatter),
      "sl_datetime"     -> today.format(dateTimeFormatter),
      "sl_year"         -> today.format(yearFormatter),
      "sl_month"        -> today.format(monthFormatter),
      "sl_day"          -> today.format(dayFormatter),
      "sl_hour"         -> today.format(hourFormatter),
      "sl_minute"       -> today.format(minuteFormatter),
      "sl_second"       -> today.format(secondFormatter),
      "sl_milli"        -> today.format(milliFormatter),
      "sl_epoch_second" -> (epochMillis / 1000).toString,
      "sl_epoch_milli"  -> epochMillis.toString
    )
  }

  def activeEnvVars(reload: Boolean = false): Map[String, String] = {
    if (reload) loadActiveEnvVars()
    this._activeEnvVars
  }

  def refs(reload: Boolean = false): RefDesc = {
    if (reload) loadRefs()
    this._refs
  }

  private var _activeEnvVars = loadActiveEnvVars()
  private var _refs = loadRefs()

  @throws[Exception]
  private def loadActiveEnvVars(): Map[String, String] = {
    val slVarsAsProperties =
      System
        .getProperties()
        .keySet()
        .iterator()
        .asScala
        .filter(_.toString.startsWith("SL_"))
        .map { key =>
          key.toString -> System.getProperties().get(key).toString
        }
        .toMap

    val externalProps = sys.env ++ slVarsAsProperties
    // We first load all variables defined in the common environment file.
    // variables defined here are default values.
    val globalsCometPath = new Path(DatasetArea.metadata, "env.sl.yml")
    val globalEnv = EnvDesc.loadEnv(globalsCometPath)(storage)
    // System Env variables may be used as values for variables defined in the env files.
    val globalEnvVars =
      globalEnv
        .map(_.env)
        .getOrElse(Map.empty)
        .mapValues(
          _.richFormat(externalProps, slDateVars)
        ) // will replace with sys.env

    val activeEnvName = Option(System.getProperties().get("env"))
      .orElse(Option(System.getenv().get("SL_ENV")))
      .orElse(globalEnvVars.get("SL_ENV"))
      .getOrElse(settings.appConfig.env)
      .toString
    // The env var SL_ENV should be set to the profile under wich starlake is run.
    // If no profile is defined, only default values are used.

    val localEnvVars =
      if (activeEnvName.nonEmpty && activeEnvName != "None") {
        val envsCometPath =
          new Path(DatasetArea.metadata, s"env.$activeEnvName.sl.yml")

        // We subsittute values defined in the current profile with variables defined
        // in the default env file

        EnvDesc
          .loadEnv(envsCometPath)(storage)
          .map(_.env)
          .getOrElse(Map.empty)
          .mapValues(
            _.richFormat(sys.env, externalProps ++ globalEnvVars ++ slDateVars)
          )

      } else
        Map.empty[String, String]

    // Please note below how profile specific vars override default profile vars.
    val activeEnvVars = externalProps ++ slDateVars ++ globalEnvVars ++ localEnvVars ++ cliEnv

    this._activeEnvVars = activeEnvVars
    this._activeEnvVars
  }

  @throws[Exception]
  private def loadRefs(): RefDesc = {
    val refsPath = new Path(DatasetArea.metadata, "refs.sl.yml")
    val refs = if (storage.exists(refsPath)) {
      val rawContent = storage.read(refsPath)
      val content = Utils.parseJinja(rawContent, activeEnvVars())
      YamlSerde.deserializeYamlRefs(content, refsPath.toString)
    } else
      RefDesc(latestSchemaVersion, settings.appConfig.refs)
    this._refs = refs
    this._refs
  }

  /** Fnd type by name
    *
    * @param tpe
    *   : Type name
    * @return
    *   Unique type referenced by this name.
    */
  def getType(tpe: String): Option[Type] = types().find(_.name == tpe)

  def deserializedDomains(
    domainPath: Path,
    domainNames: List[String] = Nil,
    raw: Boolean = false
  ): List[(Path, Try[Domain])] = {
    val directories = storage.listDirectories(domainPath)
    val requestedDomainDirectories =
      if (domainNames.isEmpty)
        directories
      else {
        directories.filter(p => domainNames.contains(p.getName()))
      }
    val domainsOnly = requestedDomainDirectories
      .map { directory =>
        val configPath = new Path(directory, "_config.sl.yml")
        if (storage.exists(configPath)) {
          val domainOnly = YamlSerde
            .deserializeYamlLoadConfig(
              if (raw) storage.read(configPath)
              else Utils.parseJinja(storage.read(configPath), activeEnvVars()),
              configPath.toString,
              isForExtract = false
            )
          val domainWithName = domainOnly
            .map { domainOnly =>
              val domainName = if (domainOnly.name == "") directory.getName() else domainOnly.name
              domainOnly.copy(name = domainName)
            }
          (configPath, domainWithName)

        } else {
          (
            configPath,
            Success(
              Domain(name = directory.getName(), comment = Some(s"${directory.getName()} domain"))
            )
          )
        }
      }
    val domains = domainsOnly.map { case (configPath, domainOnly) =>
      // grants list can be set a comma separated list in YAML , and transformed to a list while parsing
      val domainCompleted = domainOnly.map { domain =>
        {
          if (raw) {
            domain
          } else {
            val tables = domain.tables.map { table => table.normalize() }
            val metadata = domain.metadata.getOrElse(Metadata())
            // ideally the emptyNull field should set during object construction but the settings
            // object is not available in the Metadata object
            val enrichedMetadata = metadata
              .copy(emptyIsNull = metadata.emptyIsNull.orElse(Some(settings.appConfig.emptyIsNull)))

            // set domain name
            val domainName =
              if (domain.name == "") configPath.getParent().getName() else domain.name
            val domainResult =
              domain.copy(name = domainName, tables = tables, metadata = Some(enrichedMetadata))
            domainResult
          }
        }
      }
      (configPath, domainCompleted)
    }
    domains
  }

  def tableNames(): List[String] = {
    domains().flatMap { d =>
      d.tables.map { t =>
        s"${d.finalName}.${t.finalName}"
      }
    }
  }

  def taskNames(): List[String] = {
    jobs().flatMap { j =>
      j.tasks.map { t =>
        s"${j.getName()}.${t.getTableName()}"
      }
    }
  }

  def findTableNames(domainName: Option[String]): List[String] = {
    val tablesFromDomain =
      domainName match {
        case Some(domainName) => {
          domains()
            .find(_.finalName.toLowerCase() == domainName.toLowerCase())
            .map { d => d.tables.map(d.finalName + "." + _.finalName) }
            .getOrElse(Nil)
        }
        case None =>
          domains().flatMap(d => d.tables.map(d.finalName + "." + _.finalName))
      }

    val tablesFromDomainOrTasks =
      if (tablesFromDomain.isEmpty) {
        domainName match {
          case Some(domainName) =>
            tasks().filter(_.domain.toLowerCase() == domainName.toLowerCase()).map(_.name)
          case None =>
            tasks().map(_.name)
        }
      } else
        tablesFromDomain

    tablesFromDomainOrTasks
  }

  /*  def findTables(
    domainName: Option[String]
  ): Either[Map[String, List[Schema]], Map[String, List[AutoTaskDesc]]] = {
    val tablesFromDomain =
      domainName match {
        case Some(domainName) =>
          val tables = domains()
            .find(_.finalName.toLowerCase() == domainName.toLowerCase())
            .map { d => d.tables }
            .getOrElse(Nil)
          Map(domainName -> tables)

        case None =>
          domains().map(d => d.finalName -> d.tables).toMap
      }

    val tablesFromDomainOrTasks =
      if (tablesFromDomain.isEmpty) {
        domainName match {
          case Some(domainName) =>
            Right(tasksMap().filterKeys(_.toLowerCase() == domainName.toLowerCase()))
          case None =>
            Right(tasksMap())
        }
      } else
        Left(tablesFromDomain)

    tablesFromDomainOrTasks
  }
   */

  def getTablesWithColumnNames(tableNames: List[String]): List[(String, TableWithNameOnly)] = {
    val objects = getObjectNames()
    tableNames.flatMap { tableFullName =>
      val tableComponents = tableFullName.split('.')
      val (domainName, tableName) = tableComponents match {
        case Array(dbName, domainName, tableName) => (domainName, tableName)
        case Array(domainName, tableName)         => (domainName, tableName)
        case Array(tableName)                     => (null, tableName)
        case _ => throw new Exception(s"Invalid table name $tableFullName")
      }
      val domainNameAndTable =
        domainName match {
          case null =>
            objects
              .flatMap(_.tables)
              .find {
                _.name.toLowerCase() == tableName.toLowerCase()
              }
              .map {
                ("", _)
              }
          case _ =>
            objects
              .find { d =>
                d.name.toLowerCase() == domainName.toLowerCase()
              }
              .flatMap { d =>
                d.tables.find(_.name.toLowerCase() == tableName.toLowerCase())
              }
              .map { t =>
                (domainName, t)
              }
        }
      domainNameAndTable
    }
  }

  def domains(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    reload: Boolean = false,
    raw: Boolean = false
  ): List[Domain] = {
    _domains match {
      case Some(domains) =>
        if (reload || raw) { // raw is used only for special use cases so we force it to reload
          val (_, domains) = initDomains(domainNames, tableNames, raw)
          domains
        } else
          domains
      case None =>
        val (_, domains) = initDomains(domainNames, tableNames, raw)
        domains
    }
  }

  private[handlers] var (_domainErrors, _domains): (List[ValidationMessage], Option[List[Domain]]) =
    (Nil, None)

  def loadExternals(): List[Domain] = {
    if (storage.exists(DatasetArea.external)) {
      loadDomains(DatasetArea.external, Nil, Nil, raw = false) match {
        case (list, Nil) => list.collect { case Success(domain) => domain }
        case (list, errors) =>
          errors.foreach {
            case Failure(err) =>
              logger.warn(
                s"There is one or more invalid Yaml files in your domains folder:${Utils.exceptionAsString(err)}"
              )
            case Success(_) => // ignore
          }
          list.collect { case Success(domain) => domain }
      }
    } else
      Nil
  }
  def deserializedDagGenerationConfigs(dagPath: Path): Map[String, DagGenerationConfig] = {
    val dagsConfigsPaths =
      storage
        .list(path = dagPath, extension = ".sl.yml", recursive = false)
        .map(_.path)
    dagsConfigsPaths.map { dagsConfigsPath =>
      val dagConfigName = dagsConfigsPath.getName().dropRight(".sl.yml".length)
      val dagFileContent = storage.read(dagsConfigsPath)
      val dagConfig = YamlSerde
        .deserializeYamlDagConfig(
          dagFileContent,
          dagsConfigsPath.toString
        ) match {
        case Success(dagConfig) => dagConfig
        case Failure(err) =>
          logger.error(
            s"Failed to load dag config in $dagsConfigsPath"
          )
          Utils.logException(logger, err)
          throw err
      }
      logger.info(s"Successfully loaded Dag config $dagConfigName in $dagsConfigsPath")
      dagConfigName -> dagConfig
    }.toMap
  }

  /** Global dag generation config can only be defined in "dags" folder in the metadata folder.
    * Override of dag generation config can be done inside domain config file at domain or table
    * level.
    */
  def loadDagGenerationConfigs(): Map[String, DagGenerationConfig] = {
    if (storage.exists(DatasetArea.dags)) {
      deserializedDagGenerationConfigs(DatasetArea.dags)
    } else {
      logger.info("No dags config provided. Use only configuration defined in domain config files.")
      Map.empty[String, DagGenerationConfig]
    }
  }

  /** All defined domains Domains are defined under the "domains" folder in the metadata folder
    */
  @throws[Exception]
  private def initDomains(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    raw: Boolean = false
  ): (List[ValidationMessage], List[Domain]) = {
    val area = DatasetArea.load
    val (validDomainsFile, invalidDomainsFiles) =
      loadDomains(area, domainNames, tableNames, raw)

    val domains = validDomainsFile
      .collect { case Success(domain) => domain }
      .map(domain => this.fromXSD(domain))

    val (nonEmptyDomains, emptyDomains) = domains.partition(_.tables.nonEmpty)
    emptyDomains.foreach { domain =>
      logger.warn(s"Domain ${domain.name} discarded because it's empty")
    }
    val nameErrors = Utils.duplicates(
      "Domain name",
      nonEmptyDomains.map(_.name),
      s"%s is defined %d times. A domain can only be defined once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val renameErrors = Utils.duplicates(
      "Domain rename",
      nonEmptyDomains.map(d => d.rename.getOrElse(d.name)),
      s"renamed domain %s is defined %d times. It can only appear once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val directoryErrors = Utils.duplicates(
      "Domain directory",
      nonEmptyDomains.flatMap(_.resolveDirectoryOpt()),
      s"%s is defined %d times. A directory can only appear once in a domain definition file."
    ) match {
      case Right(_) =>
        if (!raw) {
          this._domains = Some(nonEmptyDomains)
          DatasetArea.initDomains(storage, nonEmptyDomains.map(_.name))

        }
        Nil
      case Left(errors) =>
        errors
    }

    invalidDomainsFiles.foreach {
      case Failure(err) =>
        logger.error(
          s"There is one or more invalid Yaml files in your domains folder:${err.getMessage}"
        )
      case Success(_) => // ignore
    }
    this._domainErrors = nameErrors ++ renameErrors ++ directoryErrors
    this._domainErrors.foreach(err => logger.error(err.toString()))
    (this._domainErrors, nonEmptyDomains)
  }

  private def loadDomains(
    area: Path,
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    raw: Boolean
  ): (List[Try[Domain]], List[Try[Domain]]) = {
    val (validDomainsFile, invalidDomainsFiles) = deserializedDomains(area, domainNames, raw)
      .map {
        case (path, Success(domain)) =>
          logger.info(s"Loading domain from $path")
          val folder = path.getParent()
          val schemaRefs = loadTableRefs(tableNames, raw, folder)
          val tables = Option(domain.tables).getOrElse(Nil) ::: schemaRefs
          val finalTables =
            if (raw) tables
            else tables.map(t => t.copy(metadata = Some(t.mergedMetadata(domain.metadata))))
          logger.info(s"Successfully loaded Domain  in $path")
          Success(domain.copy(tables = finalTables))
        case (path, Failure(e)) =>
          logger.error(s"Failed to load domain in $path")
          Utils.logException(logger, e)
          Failure(e)
      }
      .partition(_.isSuccess)
    (validDomainsFile, invalidDomainsFiles)
  }

  def loadTableRefs(
    tableNames: List[String] = Nil,
    raw: Boolean,
    folder: Path
  ): List[Schema] = {
    val tableRefNames =
      storage
        .list(folder, extension = ".sl.yml", recursive = true)
        .map(_.path.getName())
        .filter(!_.startsWith("_config."))

    val requestedTables =
      if (tableNames.isEmpty)
        tableRefNames
      else {
        tableRefNames.filter(tableNames.map(_ + ".sl.yml").contains(_))
      }

    val schemaRefs = requestedTables
      .map { tableRefName => loadTableRef(tableRefName, raw, folder) }
      .map(_.table)
    schemaRefs
  }

  def loadTableRef(
    tableRefName: String,
    raw: Boolean,
    folder: Path
  ): TableDesc = {
    val schemaPath = new Path(folder, tableRefName)
    logger.debug(s"Loading schema from $schemaPath")
    YamlSerde
      .deserializeYamlTables(
        if (raw)
          storage.read(schemaPath)
        else
          Utils
            .parseJinja(storage.read(schemaPath), activeEnvVars()),
        schemaPath.toString
      )
      .head
  }

  private def checkVarsAreDefined(path: Path): Set[ValidationMessage] = {
    val content = storage.read(path)
    checkVarsAreDefined(path, content)
  }

  private def checkVarsAreDefined(path: Path, content: String): Set[ValidationMessage] = {
    val vars = content.extractVars()
    val envVars = activeEnvVars().keySet
    val undefinedVars = vars.diff(envVars)
    undefinedVars.map(undefVar =>
      ValidationMessage(
        Warning,
        "Variable",
        s"""${path.getName} contains undefined var: ${undefVar}"""
      )
    )
  }

  def loadJobTasksFromFile(jobPath: Path): Try[AutoJobDesc] =
    for {
      jobDesc   <- loadJobDesc(jobPath)
      taskDescs <- loadJobTasks(jobDesc, jobPath.getParent)
    } yield taskDescs

  def loadJobTasks(
    jobDesc: AutoJobDesc,
    jobFolder: Path
  ): Try[AutoJobDesc] = {
    Try {
      // Load task refs and inject them in the job
      val autoTasksRefs = loadTaskRefs(jobDesc, jobFolder)

      val jobDescWithTaskRefs: AutoJobDesc =
        jobDesc.copy(tasks = Option(jobDesc.tasks).getOrElse(Nil) ::: autoTasksRefs)

      // set task name / domain / table and load sql/py file if any
      val tasks = jobDescWithTaskRefs.tasks.map { taskDesc =>
        val (taskName, tableName) = (taskDesc.name, taskDesc.table) match {
          case ("", "") =>
            throw new Exception(
              s"Task name or table must be defined for $taskDesc in ${jobFolder}"
            )
          case ("", table) =>
            (table, table)
          case (name, "") =>
            (name, name)
          case (name, table) =>
            (name, table)
        }

        // Domain name may be set explicitly or else we use the folder name
        val domainName =
          if (taskDesc.domain.isEmpty) jobFolder.getName() else taskDesc.domain

        val filenamePrefix =
          if (taskDesc._filenamePrefix.isEmpty) taskName else taskDesc._filenamePrefix
        // task name is set explicitly and we always prefix it with the domain name except if it is already prefixed.
        val taskWithName = taskDesc.copy(
          domain = domainName,
          table = tableName,
          name = s"${domainName}.${taskName}",
          _filenamePrefix = filenamePrefix
        )

        val taskCommandFile: Option[Path] =
          taskCommandPath(jobFolder, taskWithName._filenamePrefix)

        val taskWithCommand = taskCommandFile
          .map { taskFile =>
            if (taskFile.toString.endsWith(".py"))
              taskWithName.copy(
                python = Some(taskFile)
              )
            else {
              val sqlTask = SqlTaskExtractor(storage.read(taskFile))
              taskWithName.copy(
                presql = sqlTask.presql,
                sql = Option(sqlTask.sql),
                postsql = sqlTask.postsql
              )
            }
          }
          .getOrElse(taskWithName)

        // grants list can be set to a comma separated list in YAML , and transformed to a list while parsing
        taskWithCommand.copy(
          rls = taskWithName.rls.map(rls => {
            val grants = rls.grants.flatMap(_.replaceAll("\"", "").split(','))
            rls.copy(grants = grants)
          }),
          acl = taskWithName.acl.map(acl => {
            val grants = acl.grants.flatMap(_.replaceAll("\"", "").split(','))
            acl.copy(grants = grants)
          })
        )
      }
      val jobName = if (jobDesc.name.isEmpty) jobFolder.getName() else jobDesc.name
      // We do not check if task has a sql file associated with it, as it can be a python task

      val mergedTasks = jobDesc.default match {
        case Some(defaultTask) =>
          tasks.map { task =>
            defaultTask.merge(task)
          }
        case None =>
          tasks
      }
      // strip comments from SQL
      val mergedTasksWithStrippedComments = mergedTasks.map { task =>
        task.copy(
          presql = task.presql.map(sql => SQLUtils.stripComments(sql)),
          sql = task.sql.map(sql => SQLUtils.stripComments(sql)),
          postsql = task.postsql.map(sql => SQLUtils.stripComments(sql))
        )
      }
      AutoJobDesc(
        name = jobName,
        tasks = mergedTasksWithStrippedComments,
        default = None
      )
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        Failure(new Exception(s"Invalid Job file: $jobFolder(${exception.getMessage})", exception))
    }
  }

  private def loadJobDesc(jobPath: Path): Try[AutoJobDesc] = Try {
    logger.info("Loading job " + jobPath)
    val fileContent = storage.read(jobPath)
    val rootContent = Utils.parseJinja(fileContent, activeEnvVars())
    YamlSerde.deserializeYamlTransform(rootContent, jobPath.toString) match {
      case Failure(exception) =>
        throw exception
      case Success(autoJobDesc) =>
        autoJobDesc
    }
  }

  private def loadTaskRefs(
    jobDesc: AutoJobDesc,
    folder: Path
  ): List[AutoTaskDesc] = {
    // List[(prefix, filename, extension)]
    val allFiles =
      storage
        .list(folder, recursive = true)
        .map(_.path.getName())
        .filter(name => !name.startsWith("_config."))
        .flatMap { filename =>
          // improve the code below to handle more than one extension
          List("sl.yml", "sql", "sql.j2", "py")
            .find(ext => filename.endsWith(s".$ext"))
            .map { ext =>
              val taskName = filename.substring(0, filename.length - s".$ext".length)
              (taskName, filename, ext)
            }
        }
    val ymlFiles = allFiles.filter { case (taskName, filename, ext) => ext == "sl.yml" }
    val sqlPyFiles = allFiles.filter { case (taskName, filename, ext) =>
      (ext == "sql" || ext == "sql.j2" || ext == "py") &&
      !ymlFiles.exists(_._1 == taskName) &&
      !jobDesc.tasks.exists(_.name.equalsIgnoreCase(taskName))
    }
    val autoTasksRefNames: List[(String, String, String)] = ymlFiles ++ sqlPyFiles
    val autoTasksRefs = autoTasksRefNames.flatMap {
      case (taskFilePrefix, taskFilename, extension) =>
        extension match {
          case "sl.yml" =>
            Try {
              val taskPath = new Path(folder, taskFilename)
              val taskDesc = YamlSerde
                .deserializeYamlTask(
                  Utils
                    .parseJinja(storage.read(taskPath), activeEnvVars()),
                  taskPath.toString
                )
                .copy(name = taskFilePrefix)
              val taskName = if (taskDesc.name.nonEmpty) taskDesc.name else taskFilePrefix
              taskDesc.copy(_filenamePrefix = taskFilePrefix, name = taskName)
            } match {
              case Failure(e) =>
                e.printStackTrace()
                logger.error(e.getMessage, e)
                // TODO: could not deserialise. Since we support legacy we may encounter sl.yml files that doesn't define task nor _config.sl.yml. We should add breaking change to remove this behavior and have a strict definition of config files.
                None
              case Success(value) => Some(value)
            }
          case _ =>
            Some(
              AutoTaskDesc(
                name = taskFilePrefix,
                sql = None,
                database = None,
                domain = "",
                table = "",
                _filenamePrefix = taskFilePrefix,
                taskTimeoutMs = None
              )
            )
        }
    }
    autoTasksRefs
  }

  /** @param basePath
    * @param filePrefix
    * @param extensions
    * @return
    *   (path, prefix, extension)
    */
  def getTaskPathDetails(
    basePath: Path,
    filePrefix: String,
    extensions: List[String]
  ): Option[(Path, String, String)] = {
    extensions.flatMap { extension =>
      val path = new Path(basePath, s"$filePrefix.$extension")
      if (storage.exists(path)) {
        val prefix = path.getName.substring(0, path.getName.length - s".$extension".length)
        Some(path, prefix, extension)
      } else
        None
    }.headOption
  }

  /** @param basePath
    * @param sqlFilePrefix
    * @return
    *   (path, prefix, extension)
    */
  private def taskCommandPath(
    basePath: Path,
    sqlFilePrefix: String
  ): Option[Path] = {
    getTaskPathDetails(basePath, sqlFilePrefix, List("sql", "sql.j2", "py")).map {
      case (path, prefix, extension) => path
    }
  }

  def deserializedJobs(jobPath: Path): List[(Path, Try[AutoJobDesc])] = {
    val paths = storage
      .list(
        jobPath,
        extension = ".sl.yml",
        recursive = true,
        exclude = Some(Pattern.compile("_.*"))
      )
      .map(_.path)

    val jobs = paths
      .map { path =>
        YamlSerde.deserializeYamlTransform(
          Utils.parseJinja(storage.read(path), activeEnvVars()),
          path.toString
        )
      }
    paths.zip(jobs)
  }

  def jobs(reload: Boolean = false): List[AutoJobDesc] = {
    if (_jobs.isEmpty || reload) {
      val (errors, validJobs) = loadJobs()
      this._jobs = validJobs
      this._jobErrors = errors
    }
    _jobs
  }

  def tasks(reload: Boolean = false): List[AutoTaskDesc] = {
    if (reload) {
      val (errors, validJobs) = loadJobs()
      this._jobs = validJobs
      this._jobErrors = errors
    }
    jobs().flatMap(_.tasks)
  }

  def tasksMap(reload: Boolean = false): Map[String, List[AutoTaskDesc]] = {
    if (reload) {
      val (errors, validJobs) = loadJobs()
      this._jobs = validJobs
      this._jobErrors = errors
    }
    jobs().map(j => j.getName() -> j.tasks).toMap
  }

  def iamPolicyTags(): Option[IamPolicyTags] = {
    val path = DatasetArea.iamPolicyTags()
    if (storage.exists(path))
      Some(YamlSerde.deserializeIamPolicyTags(storage.read(path)))
    else
      None
  }

  def task(taskName: String): Option[AutoTaskDesc] = {
    val allTasks = tasks()
    allTasks.find(t => t.name == taskName)
  }

  private var (_jobErrors, _jobs): (List[ValidationMessage], List[AutoJobDesc]) = (Nil, Nil)

  /** All defined jobs Jobs are defined under the "jobs" folder in the metadata folder
    */
  @throws[Exception]
  private def loadJobs(): (List[ValidationMessage], List[AutoJobDesc]) = {
    if (storage.exists(DatasetArea.transform)) {
      val directories = storage.listDirectories(DatasetArea.transform)
      val (validJobsFile, invalidJobsFile) = directories
        .map { directory =>
          val configPath = new Path(directory, "_config.sl.yml")
          if (storage.exists(configPath)) {
            val result = loadJobTasksFromFile(configPath)
            result match {
              case Success(_) =>
                logger.info(s"Successfully loaded Job $configPath")
              case Failure(e) =>
                logger.error(s"Failed to load Job $configPath")
                e.printStackTrace()
            }
            result
          } else {
            logger.info(s"Job $directory does not have a _config.sl.yml file")
            val job = AutoJobDesc(directory.getName(), Nil)
            val result = loadJobTasks(job, directory)
            result match {
              case Success(_) =>
                logger.info(s"Successfully loaded Job $directory")
              case Failure(e) =>
                logger.error(s"Failed to load Job $directory")
                e.printStackTrace()
            }
            result
          }
        }
        .partition(_.isSuccess)

      val validJobs = validJobsFile
        .collect { case Success(job) => job }

      val namePatternErrors = validJobs.filter(_.name.nonEmpty).map(_.name).flatMap { name =>
        if (!forceJobPrefixRegex.pattern.matcher(name).matches())
          Some(
            ValidationMessage(
              Error,
              "Transform",
              s"Transform with name $name should respect the pattern ${forceJobPrefixRegex.regex}"
            )
          )
        else
          None
      }

      val taskNamePatternErrors =
        validJobs.flatMap(_.tasks.filter(_.name.nonEmpty).map(_.name)).flatMap { name =>
          val components = name.split('.')
          if (components.length != 2) {
            Some(
              ValidationMessage(
                Error,
                "Task",
                s"Tasks with name $name should be prefixed with the domain name"
              )
            )
          } else if (!forceTaskPrefixRegex.pattern.matcher(components(1)).matches())
            Some(
              ValidationMessage(
                Error,
                "Task",
                s"Tasks with name ${components(1)} in domain ${components(0)} should respect the pattern ${forceTaskPrefixRegex.regex}"
              )
            )
          else
            None
        }
      (namePatternErrors ++ taskNamePatternErrors, validJobs)
    } else {
      (Nil, Nil)
    }
  }

  /** Find domain by name
    *
    * @param name
    *   : Domain name
    * @return
    *   Unique Domain referenced by this name.
    */
  def getDomain(name: String, raw: Boolean = false): Option[Domain] =
    domains(domainNames = List(name), raw = raw).find(_.name == name)

  /** Return all schemas for a domain
    *
    * @param domain
    *   : Domain name
    * @return
    *   List of schemas for a domain, empty list if no schema or domain is found
    */
  def getSchemas(domain: String): List[Schema] = getDomain(domain).map(_.tables).getOrElse(Nil)

  /** Get schema by name for a domain
    *
    * @param domainName
    *   : Domain name
    * @param schemaName
    *   : Sceham name
    * @return
    *   Unique Schema with this name for a domain
    */
  def getSchema(domainName: String, schemaName: String): Option[Schema] =
    for {
      domain <- getDomain(domainName)
      schema <- domain.tables.find(_.name == schemaName)
    } yield schema

  def fromXSD(domain: Domain): Domain = {
    val domainMetadata = domain.metadata
    val tables = domain.tables.map { table =>
      fromXSD(table, domainMetadata)
    }
    domain.copy(tables = tables)
  }

  def fromXSD(ymlSchema: Schema, domainMetadata: Option[Metadata]): Schema = {
    val metadata = ymlSchema.mergedMetadata(domainMetadata)
    metadata.getXsdPath() match {
      case None =>
        ymlSchema
      case Some(xsd) =>
        val xsdContent = storage.read(new Path(xsd))
        val sparkType = XSDToSchema.read(xsdContent)
        val topElement = sparkType.fields.map(field => Attribute(field))
        val (nonScripted, scripted) = topElement.head.attributes.partition(_.script.isEmpty)
        val xsdAttributes = nonScripted ++ scripted
        // val xsdAttributes = sordtedAttrs.head.attributes
        val merged = Attribute.mergeAll(
          xsdAttributes,
          ymlSchema.attributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = true,
            keepSourceDiffAttributesStrategy =
              DropAll, // Keep existing behavior but wonder if we should use KeepAllDiff
            attributePropertiesMergeStrategy = SourceFirst
          )
        )(this)
        ymlSchema.copy(attributes = merged)
    }
  }

  def getFullTableName(domain: Domain, schema: Schema)(implicit
    settings: Settings
  ): String = {
    val databaseName = domain.database.orElse(settings.appConfig.getDefaultDatabase())
    databaseName match {
      case Some(db) =>
        s"$db.${domain.finalName}.${schema.finalName}"
      case None =>
        s"${domain.finalName}.${schema.finalName}"
    }
  }

  def getDatabase(domain: Domain)(implicit settings: Settings): Option[String] =
    domain.database.orElse(settings.appConfig.getDefaultDatabase())
  // SL_DATABASE
  // default database

  def getObjectNames(): List[DomainWithNameOnly] = {
    val domains = this.domains() ++ this.loadExternals() ++ List(this.auditTables)
    val tableNames =
      domains.map { domain =>
        DomainWithNameOnly(
          domain.finalName,
          domain.tables
            .map { table =>
              TableWithNameOnly(table.finalName, table.attributes.map(_.getFinalName()).sorted)
            }
            .sortBy(_.name)
        )
      }
    val jobs = this.jobs()
    val taskNames =
      jobs
        .map { job =>
          DomainWithNameOnly(
            job.name,
            job.tasks.map(_.table).sorted.map(TableWithNameOnly(_, List.empty))
          )
        }
    val all = tableNames ++ taskNames
    val result = all.sortBy(_.name)
    result
  }

  val auditTables: Domain =
    Domain(
      settings.appConfig.audit.getDomain(),
      tables = List(
        AuditLog.starlakeSchema,
        ExpectationReport.starlakeSchema,
        RejectedRecord.starlakeSchema
      )
    )
  def onTaskDelete(domain: String, task: String): Unit = {
    _jobs.find(_.name.toLowerCase() == domain.toLowerCase()) match {
      case None =>
        logger.info(s"Job $domain not found")
      case Some(domain) =>
        val tasks = domain.tasks.filterNot(_.name.toLowerCase() == task.toLowerCase())
        val newDomain = domain.copy(tasks = tasks)
        _jobs = _jobs.filterNot(_.name.toLowerCase() == domain.name.toLowerCase()) :+ newDomain
    }
  }
  def onTableDelete(domain: String, table: String): Unit = {
    _domains.getOrElse(Nil).find(_.name.toLowerCase() == domain.toLowerCase()) match {
      case None =>
        logger.warn(s"Domain $domain not found")
      case Some(domain) =>
        val tablesToKeep = domain.tables.filterNot(_.name.toLowerCase() == table.toLowerCase())
        val updatedDomain = domain.copy(tables = tablesToKeep)
        _domains = _domains
          .map(_.filterNot(_.name.toLowerCase() == domain.name.toLowerCase()) :+ updatedDomain)
          .orElse(Some(List(updatedDomain)))
    }
  }

  def onTaskChange(domain: String, task: String, file: File): Unit = {
    /*
    val domainPath = new Path(DatasetArea.transform, domain)
    val taskPath = new Path(domainPath, task + ".sl.yml")
    if (storage.exists(taskPath)) {
      val task = loadJobTasksFromFile(taskPath)
      task match {
        case Success(task) =>
          _jobs.find(_.name.toLowerCase() == domain.toLowerCase()) match {
            case None =>
              logger.info(s"Job $domain not found")
              val (validationMessages, validJobsFile) =
                this.loadJobs(DatasetArea.transform, List(domain), Nil)
              _jobs = _jobs ++ validJobsFile
            case Some(job) =>
              val tasks =
                job.tasks.filterNot(_.name.toLowerCase() == task.name.toLowerCase()) :+ task
              val newJob = job.copy(tasks = tasks)
              _jobs = _jobs.filterNot(_.name.toLowerCase() == job.name.toLowerCase()) :+ newJob

          }
        case Failure(err) =>
          logger.error(s"Failed to load task $taskPath")
          Utils.logException(logger, err)
      }
    }
     */
  }

  def onTableChange(updatedDomain: String, updatedTable: String, file: File): Unit = {
    val updatedDomainPath = new Path(DatasetArea.load, updatedDomain)
    val updatedTablePath = new Path(updatedDomainPath, updatedTable + ".sl.yml")
    if (storage.exists(updatedTablePath)) {
      val updatedTableReloaded = Try(
        loadTableRefs(List(updatedTable), raw = false, folder = updatedDomainPath).head
      )
      updatedTableReloaded match {
        case Success(updatedTable) =>
          _domains.getOrElse(Nil).find(_.name.toLowerCase() == updatedDomain.toLowerCase()) match {
            case None =>
              logger.warn(s"Domain $updatedDomain not found")
              val (validDomainsFile, invalidDomainsFiles) =
                this.loadDomains(DatasetArea.load, List(updatedDomain), Nil, raw = false)
              val newDomains = validDomainsFile.collect { case Success(domain) => domain }
              _domains = Some(_domains.getOrElse(Nil) ++ newDomains)
            case Some(domain) =>
              val unchangedTables =
                domain.tables.filterNot(
                  _.name.toLowerCase() == updatedTable.name.toLowerCase()
                )
              val newDomain = domain.copy(tables = unchangedTables :+ updatedTable)
              _domains = Some(_domains.get.filterNot(_.name == domain.name) :+ newDomain)
          }
        case Failure(err) =>
          logger.error(s"Failed to load table $updatedTablePath")
          Utils.logException(logger, err)
      }
    }
  }
}
