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

import ai.starlake.config.Settings.AppConfig
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model.Severity._
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.{StarlakeObjectMapper, Utils, YamlSerializer}
import better.files.File
import com.databricks.spark.xml.util.XSDToSchema
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.annotation.nowarn
import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/** Handles access to datasets metadata, eq. domains / types / schemas.
  *
  * @param storage
  *   : Underlying filesystem manager
  */
class SchemaHandler(storage: StorageHandler, cliEnv: Map[String, String] = Map.empty)(implicit
  settings: Settings
) extends StrictLogging {

  private val forceViewPrefixRegex: Regex = settings.appConfig.forceViewPattern.r
  private val forceJobPrefixRegex: Regex = settings.appConfig.forceJobPattern.r
  private val forceTaskPrefixRegex: Regex = settings.appConfig.forceTablePattern.r

  // uses Jackson YAML for parsing, relies on SnakeYAML for low level handling
  @nowarn val mapper: ObjectMapper with ScalaObjectMapper =
    new StarlakeObjectMapper(new YAMLFactory(), injectables = (classOf[Settings], settings) :: Nil)

  @throws[Exception]
  private def checkTypeDomainsJobsValidity(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    reload: Boolean = false
  )(implicit storage: StorageHandler): List[ValidationMessage] = {

    val domainStructureValidity = Domain.checkFilenamesValidity()(storage, settings)
    val types = this.types(reload)
    if (types.isEmpty) {
      throw new Exception("No types defined. Please define types in metadata/types/default.sl.yml")
    }

    val typesValidity = types.map(_.checkValidity())
    val loadedDomains = this.domains(domainNames, tableNames, reload)
    val loadedDomainsValidity = loadedDomains.map(_.checkValidity(this))
    val domainsValidity =
      domainStructureValidity ++ loadedDomainsValidity
    val domainsVarsValidity = checkDomainsVars()
    val jobsVarsValidity = checkJobsVars() // job vars may be defined at runtime.
    val allErrors = typesValidity ++ domainsValidity :+ checkViewsValidity()

    val errs = allErrors.flatMap {
      case Left(values) => values
      case Right(_)     => Nil
    }
    errs ++ domainsVarsValidity ++ jobsVarsValidity
  }

  private def checkDomainsVars(): List[ValidationMessage] = {
    val paths = storage.list(
      DatasetArea.load,
      extension = ".sl.yml",
      recursive = true,
      exclude = Some(Pattern.compile("_.*"))
    )
    paths.flatMap(checkVarsAreDefined)
  }

  def checkJobsVars(): List[ValidationMessage] = {
    val paths = storage.list(DatasetArea.transform, ".sl.yml", recursive = true)
    val ymlWarnings = paths.flatMap(checkVarsAreDefined)
    val sqlPaths =
      storage.list(DatasetArea.transform, ".sql.j2", recursive = true) ++ storage.list(
        DatasetArea.transform,
        ".sql",
        recursive = true
      ) ++ storage.list(
        DatasetArea.transform,
        ".py",
        recursive = true
      )
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
  ): Try[(Int, Int)] = Try {
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

    val expectationErrors = ExpectationDefinition.checkValidity(expectations().values.toList)
    val allErrorsAndWarnings =
      settingsErrorsAndWarnings ++ typesDomainsJobsErrorsAndWarnings ++ deserErrors ++ this._domainErrors ++ this._jobErrors ++ expectationErrors
    val (warnings, errors) = allErrorsAndWarnings.partition(_.severity == Warning)
    val errorCount = errors.length
    val warningCount = warnings.length

    val output =
      settings.appConfig.rootServe.map(rootServe => File(File(rootServe), "validation.log"))
    output.foreach(_.overwrite(""))

    if (errorCount + warningCount > 0) {
      output.foreach(
        _.appendLine(
          s"START VALIDATION RESULTS: $errorCount errors and $warningCount found"
        )
      )
      logger.error(s"START VALIDATION RESULTS: $errorCount errors  and $warningCount found")
      allErrorsAndWarnings.foreach { err =>
        if (err.severity == Warning)
          logger.warn(err.message)
        else
          logger.error(err.message)
        output.foreach(_.appendLine(err.message))
      }
      logger.error(s"END VALIDATION RESULTS")
      output.foreach(_.appendLine(s"END VALIDATION RESULTS"))
      if (settings.appConfig.validateOnLoad)
        throw new Exception(
          s"Validation Failed: $errorCount errors and $warningCount warning found"
        )
    }
    (errorCount, warningCount)
  }

  def checkViewsValidity(): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.MutableList[ValidationMessage] = mutable.MutableList.empty
    val viewsPath = DatasetArea.views
    val sqlFiles =
      storage.list(viewsPath, extension = ".sql", recursive = true) ++
      storage.list(viewsPath, extension = ".sql.j2", recursive = true)

    val duplicates = sqlFiles.groupBy(_.getName).filter { case (name, paths) => paths.length > 1 }
    // Check Domain name validity
    sqlFiles.foreach { sqlFile =>
      val name = viewName(sqlFile)
      if (!forceViewPrefixRegex.pattern.matcher(name).matches()) {
        errorList += ValidationMessage(
          Error,
          "View",
          s"View with name $name should respect the pattern ${forceViewPrefixRegex.regex}"
        )
      }
    }

    duplicates.foreach { duplicate =>
      errorList += ValidationMessage(Error, "View", s"Found duplicate views => $duplicate")
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)

  }
  def loadTypes(filename: String): List[Type] = {
    val deprecatedTypesPath = new Path(DatasetArea.types, filename + ".yml")
    val typesCometPath = new Path(DatasetArea.types, filename + ".sl.yml")
    if (storage.exists(typesCometPath))
      mapper.readValue(storage.read(typesCometPath), classOf[Types]).types
    else if (storage.exists(deprecatedTypesPath))
      mapper.readValue(storage.read(deprecatedTypesPath), classOf[Types]).types
    else
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
    val defaultTypes = loadTypes("default") :+ Type("struct", ".*", PrimitiveType.struct)
    val types = loadTypes("types")

    val redefinedTypeNames =
      defaultTypes.map(_.name).intersect(types.map(_.name))

    this._types =
      defaultTypes.filter(defaultType => !redefinedTypeNames.contains(defaultType.name)) ++ types
    this._types
  }

  def loadExpectations(filename: String): Map[String, ExpectationDefinition] = {
    val expectationsPath = new Path(DatasetArea.expectations, filename)
    logger.info(s"Loading expectations $expectationsPath")
    if (storage.exists(expectationsPath)) {
      val content = Utils
        .parseJinja(storage.read(expectationsPath), activeEnvVars())
      mapper
        .readValue(content, classOf[ExpectationDefinitions])
        .expectationDefinitions
    } else
      Map.empty[String, ExpectationDefinition]
  }

  def loadExternalSources(filename: String): List[ExternalDatabase] = {
    val externalPath = new Path(DatasetArea.external, filename)
    logger.info(s"Loading external $externalPath")
    if (storage.exists(externalPath)) {
      val content = Utils
        .parseJinja(storage.read(externalPath), activeEnvVars())
      mapper
        .readValue(content, classOf[ExternalSource])
        .projects
    } else
      List.empty[ExternalDatabase]
  }

  @throws[Exception]
  def externalSources(): List[ExternalDatabase] = loadExternalSources("default.sl.yml")

  @throws[Exception]
  def expectations(name: String): Map[String, ExpectationDefinition] = {
    val defaultExpectations = loadExpectations("default.sl.yml")
    val expectations = loadExpectations("expectations.sl.yml")
    val resExpectations = loadExpectations(name + ".sl.yml")
    defaultExpectations ++ expectations ++ resExpectations
  }

  @throws[Exception]
  def expectations(): Map[String, ExpectationDefinition] = {
    val defaultExpectations = loadExpectations("default.sl.yml")
    val expectations = loadExpectations("expectations.sl.yml")
    val domainExpectations = this.domains().flatMap { domain =>
      loadExpectations(domain.name + ".sl.yml")
    }

    defaultExpectations ++ expectations ++ domainExpectations
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
    val viewFile = listSqlj2Files(path).find(_.getName().startsWith(s"$viewName.sql"))
    viewFile.map { viewFile =>
      val (viewName, viewContent) = loadSqlJ2File(viewFile)
      viewContent
    }
  }

  private def listSqlj2Files(path: Path) = {
    val j2Files = storage.list(path, extension = ".sql.j2", recursive = true)
    val sqlFiles = storage.list(path, extension = ".sql", recursive = true)
    val allFiles = j2Files ++ sqlFiles
    allFiles
  }

  private def loadSqlJ2Files(path: Path): Map[String, String] = {
    val allFiles = listSqlj2Files(path)
    allFiles.map { sqlFile =>
      loadSqlJ2File(sqlFile)
    }.toMap
  }

  def views(): Map[String, String] = loadSqlJ2Files(DatasetArea.views)

  def view(viewName: String): Option[String] = loadSqlJ2File(DatasetArea.views, viewName)

  val cometDateVars: Map[String, String] = {
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
      "comet_date"         -> today.format(dateFormatter),
      "comet_datetime"     -> today.format(dateTimeFormatter),
      "comet_year"         -> today.format(yearFormatter),
      "comet_month"        -> today.format(monthFormatter),
      "comet_day"          -> today.format(dayFormatter),
      "comet_hour"         -> today.format(hourFormatter),
      "comet_minute"       -> today.format(minuteFormatter),
      "comet_second"       -> today.format(secondFormatter),
      "comet_milli"        -> today.format(milliFormatter),
      "comet_epoch_second" -> (epochMillis / 1000).toString,
      "comet_epoch_milli"  -> epochMillis.toString
    )
  }

  def activeEnvVars(reload: Boolean = false): Map[String, String] = {
    if (reload) loadActiveEnvVars()
    this._activeEnvVars
  }

  def refs(reload: Boolean = false): Refs = {
    if (reload) loadRefs()
    this._refs
  }

  private var _activeEnvVars = loadActiveEnvVars()
  private var _refs = loadRefs()

  @throws[Exception]
  private def loadActiveEnvVars(): Map[String, String] = {
    def loadEnv(path: Path): Option[Env] =
      if (storage.exists(path))
        Option(mapper.readValue(storage.read(path), classOf[Env]))
      else {
        logger.warn(s"Env file $path not found")
        None
      }

    // We first load all variables defined in the common environment file.
    // variables defined here are default values.
    val globalsCometPath = new Path(DatasetArea.metadata, s"env.sl.yml")
    val globalEnv = loadEnv(globalsCometPath)
    // System Env variables may be used as values for variables defined in the env files.
    val globalEnvVars =
      globalEnv
        .map(_.env)
        .getOrElse(Map.empty)
        .mapValues(
          _.richFormat(sys.env, cometDateVars)
        ) // will replace with sys.env
    val activeEnvName = Option(System.getenv().get("SL_ENV"))
      .orElse(globalEnvVars.get("SL_ENV"))
      .getOrElse(settings.appConfig.env)
    // The env var SL_ENV should be set to the profile under wich starlake is run.
    // If no profile is defined, only default values are used.

    val localEnvVars =
      if (activeEnvName.nonEmpty) {
        val envsCometPath = new Path(DatasetArea.metadata, s"env.$activeEnvName.sl.yml")

        // We subsittute values defined in the current profile with variables defined
        // in the default env file

        loadEnv(envsCometPath)
          .map(_.env)
          .getOrElse(Map.empty)
          .mapValues(_.richFormat(sys.env, globalEnvVars ++ cometDateVars))
      } else
        Map.empty[String, String]

    // Please note below how profile specific vars override default profile vars.
    val activeEnvVars = sys.env ++ cometDateVars ++ globalEnvVars ++ localEnvVars ++ cliEnv

    this._activeEnvVars = activeEnvVars
    this._activeEnvVars
  }

  @throws[Exception]
  private def loadRefs(): Refs = {
    val refsPath = new Path(DatasetArea.metadata, "refs.sl.yml")
    val refs = if (storage.exists(refsPath)) {
      val rawContent = storage.read(refsPath)
      val content = Utils.parseJinja(rawContent, activeEnvVars())
      YamlSerializer.mapper.readValue(content, classOf[Refs])
    } else
      Refs(settings.appConfig.refs)
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
          val domainOnly = YamlSerializer
            .deserializeDomain(
              if (raw) storage.read(configPath)
              else Utils.parseJinja(storage.read(configPath), activeEnvVars()),
              configPath.toString
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
            Failure(new RuntimeException(s"Config file not found in ${directory.toString}"))
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
            val tables = domain.tables.map { table =>
              table.copy(
                rls = table.rls.map(rls => {
                  val grants = rls.grants.flatMap(_.replaceAll("\"", "").split(','))
                  rls.copy(grants = grants)
                }),
                acl = table.acl.map(acl => {
                  val grants = acl.grants.flatMap(_.replaceAll("\"", "").split(','))
                  acl.copy(grants = grants)
                })
              )
            }
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

  def findTableNames(domainName: Option[String], tableNamePrefix: String): List[String] = {
    val loadedTables = {
      domainName match {
        case Some(domainName) => {
          domains()
            .find(_.finalName.toLowerCase() == domainName.toLowerCase())
            .map(_.tables.map(_.finalName))
            .getOrElse(Nil)
        }
        case None =>
          domains().flatMap(_.tables.map(_.finalName))
      }
    }

    val tables =
      if (loadedTables.isEmpty) {
        domainName match {
          case Some(domainName) =>
            tasks().filter(_.domain.toLowerCase() == domainName.toLowerCase()).map(_.table)
          case None =>
            tasks().map(_.table)
        }
      } else
        loadedTables

    tables.filter { name =>
      name.toLowerCase().startsWith(tableNamePrefix.toLowerCase())
    }
  }

  /** @param domainName
    * @param tableName
    * @param ColumnPrefix
    * @return
    */
  def findTableColumnNames(
    domainName: Option[String],
    tableName: String,
    ColumnPrefix: String
  ): List[String] = {
    val loadedTables = {
      domainName match {
        case Some(domainName) => {
          domains()
            .find(_.finalName.toLowerCase() == domainName.toLowerCase())
            .map(_.tables.map(_.finalName))
            .getOrElse(Nil)
        }
        case None =>
          domains().flatMap(_.tables.map(_.finalName))
      }
    }

    val tables =
      if (loadedTables.isEmpty)
        tasks().filter(_.domain.toLowerCase() == domainName.toLowerCase()).map(_.table)
      else
        loadedTables

    tables.filter { name =>
      name.toLowerCase().startsWith(tableNamePrefix.toLowerCase())
    }

  }

  def findTableNames(tableNamePrefix: String): List[String] = {
    val loadedTables = domains().flatMap(_.tables.map(_.finalName))
    val taskTables = tasks().map(_.table)

    (loadedTables ++ taskTables)
      .filter { table =>
        table.toLowerCase().startsWith(tableNamePrefix.toLowerCase())
      }
  }

  def findDomainNames(domainNamePrefix: String): List[String] = {
    (domains().map(_.finalName) ++ tasks().map(_.domain))
      .filter { name =>
        name.toLowerCase().startsWith(domainNamePrefix.toLowerCase())
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

  private var (_domainErrors, _domains): (List[ValidationMessage], Option[List[Domain]]) =
    (Nil, None)

  private def initDomains(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    raw: Boolean = false
  ): (List[ValidationMessage], List[Domain]) = {
    initDomainsFromArea(DatasetArea.load, domainNames, tableNames, raw)
  }

  def loadExternals(): (List[ValidationMessage], List[Domain]) = {
    initDomainsFromArea(DatasetArea.external)
  }

  def deserializedDagGenerationConfigs(dagPath: Path): Map[String, DagGenerationConfig] = {
    val dagsConfigsPaths = storage.list(path = dagPath, extension = ".sl.yml", recursive = false)
    dagsConfigsPaths.map { dagsConfigsPath =>
      val dagConfigName = dagsConfigsPath.getName().dropRight(".sl.yml".length)
      val dagFileContent = storage.read(dagsConfigsPath)
      val dagConfig = YamlSerializer
        .deserializeDagGenerationConfig(
          dagFileContent,
          dagsConfigsPath.toString
        ) match {
        case Success(dagConfig) =>
          // we save the raw filename since it willl be instantiaed at runtime (it holds the domain and table vars potentially
          val rawFilename = dagConfig.filename
          // Let's reload the dag config and apply the jinja templating
          val dagConfigResult = YamlSerializer
            .deserializeDagGenerationConfig(
              Utils.parseJinja(dagFileContent, activeEnvVars()),
              dagsConfigsPath.toString
            )
          dagConfigResult match {
            case Success(dagConfig) =>
              dagConfig.copy(filename = rawFilename)
            case Failure(err) =>
              throw err
          }
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
  private def initDomainsFromArea(
    area: Path,
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    raw: Boolean = false
  ): (List[ValidationMessage], List[Domain]) = {
    val (validDomainsFile, invalidDomainsFiles) =
      loadFullDomains(area, domainNames, tableNames, raw)

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

  private def loadFullDomains(
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

  private def loadTableRefs(
    tableNames: List[String] = Nil,
    raw: Boolean,
    folder: Path
  ): List[Schema] = {
    val tableRefNames =
      storage
        .list(folder, extension = ".sl.yml", recursive = true)
        .map(_.getName())
        .filter(!_.startsWith("_config."))

    val requestedTables =
      if (tableNames.isEmpty)
        tableRefNames
      else {
        tableRefNames.filter(tableNames.map(_ + ".sl.yml").contains(_))
      }
    val schemaRefs = requestedTables
      .map { tableRefName =>
        val schemaPath = new Path(folder, tableRefName)
        YamlSerializer.deserializeSchemaRefs(
          if (raw)
            storage.read(schemaPath)
          else
            Utils
              .parseJinja(storage.read(schemaPath), activeEnvVars()),
          schemaPath.toString
        )
      }
      .flatMap(_.tables)
    schemaRefs
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

  def loadJobTasksFromFile(jobPath: Path): Try[AutoJobDesc] = {
    val jobDesc = loadJobDesc(jobPath)
    logger.info(s"Successfully loaded job  in $jobPath")
    val jobParentPath = jobPath.getParent()
    loadJobTasks(jobDesc, jobParentPath)
  }

  def loadJobTasks(jobDesc: AutoJobDesc, jobFolder: Path): Try[AutoJobDesc] = {
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
      AutoJobDesc(
        name = jobName,
        tasks = mergedTasks,
        default = None
      )
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        Failure(new Exception(s"Invalid Job file: $jobFolder(${exception.getMessage})", exception))
    }
  }

  private def loadJobDesc(jobPath: Path): AutoJobDesc = {
    val fileContent = storage.read(jobPath)
    val rootContent = Utils.parseJinja(fileContent, activeEnvVars())
    val rootNode = mapper.readTree(rootContent)
    val tranformNode = rootNode.path("transform")
    val autojobNode =
      if (tranformNode.isNull() || tranformNode.isMissingNode) {
        logger.warn(
          s"Defining an autojob outside a transform node is now deprecated. Please update definition $jobPath"
        )
        rootNode
      } else
        tranformNode
    val taskPathNode = autojobNode.path("tasks")
    if (!taskPathNode.isMissingNode) {
      val tasksNode = taskPathNode.asInstanceOf[ArrayNode]
      for (i <- 0 until tasksNode.size()) {
        val taskNode: ObjectNode = tasksNode.get(i).asInstanceOf[ObjectNode]
        YamlSerializer.upgradeTaskNode(taskNode)
      }
    }
    val jobDesc = mapper.treeToValue(autojobNode, classOf[AutoJobDesc])
    jobDesc
  }

  private def loadTaskRefs(jobDesc: AutoJobDesc, folder: Path): List[AutoTaskDesc] = {
    // List[(prefix, filename, extension)]
    val allFiles =
      storage
        .list(folder, recursive = true)
        .map(_.getName())
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
    val ymlFiles = allFiles.filter(_._3 == "sl.yml")
    val sqlPyFiles = allFiles.filter(x =>
      (x._3 == "sql" || x._3 == "sql.j2" || x._3 == "py")
      && !ymlFiles.exists(_._1 == x._1)
      && !jobDesc.tasks.exists(_.name == x._1)
    )
    val autoTasksRefNames: List[(String, String, String)] = ymlFiles ++ sqlPyFiles
    val autoTasksRefs = autoTasksRefNames.map { case (taskFilePrefix, taskFilename, extension) =>
      extension match {
        case "sl.yml" =>
          val taskPath = new Path(folder, taskFilename)
          val taskNode = loadTaskRefNode(
            Utils
              .parseJinja(storage.read(taskPath), activeEnvVars())
          )
          YamlSerializer.upgradeTaskNode(taskNode)
          val taskDesc = YamlSerializer.deserializeTaskNode(taskNode).copy(name = taskFilePrefix)
          val taskName = if (taskDesc.name.nonEmpty) taskDesc.name else taskFilePrefix
          taskDesc.copy(_filenamePrefix = taskFilePrefix, name = taskName)
        case _ =>
          AutoTaskDesc(
            name = taskFilePrefix,
            sql = None,
            database = None,
            domain = "",
            table = "",
            write = None,
            _filenamePrefix = taskFilePrefix
          )
      }
    }
    autoTasksRefs
  }

  private def loadTaskRefNode(content: String): ObjectNode = {
    val rootNode = mapper.readTree(content)
    val taskNode = rootNode.path("task")
    if (taskNode.isNull() || taskNode.isMissingNode) // backward compatibility
      rootNode.asInstanceOf[ObjectNode]
    else
      taskNode.asInstanceOf[ObjectNode]
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
  ): Option[(Path)] = {
    getTaskPathDetails(basePath, sqlFilePrefix, List("sql", "sql.j2", "py")).map {
      case (path, prefix, extension) => path
    }
  }

  def deserializedJobs(jobPath: Path): List[(Path, Try[AutoJobDesc])] = {
    val paths = storage.list(
      jobPath,
      extension = ".sl.yml",
      recursive = true,
      exclude = Some(Pattern.compile("_.*"))
    )

    val jobs = paths
      .map { path =>
        YamlSerializer.deserializeJob(
          Utils.parseJinja(storage.read(path), activeEnvVars()),
          path.toString
        )
      }
    paths.zip(jobs)
  }

  def jobs(reload: Boolean = false): List[AutoJobDesc] = {
    if (reload) loadJobs()
    _jobs
  }

  def tasks(reload: Boolean = false): List[AutoTaskDesc] = {
    if (reload) loadJobs()
    jobs().flatMap(_.tasks)
  }

  def iamPolicyTags(): Option[IamPolicyTags] = {
    val path = DatasetArea.iamPolicyTags()
    if (storage.exists(path))
      Some(YamlSerializer.deserializeIamPolicyTags(storage.read(path)))
    else
      None
  }

  def task(taskName: String): Option[AutoTaskDesc] = {
    val allTasks = tasks()
    allTasks.find(t => t.name == taskName)
  }

  private var (_jobErrors, _jobs): (List[ValidationMessage], List[AutoJobDesc]) = loadJobs()

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
            logger.info(s"Job $directory does not have a _config.yml file")
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
      this._jobs = validJobs
      this._jobErrors = namePatternErrors ++ taskNamePatternErrors
      (_jobErrors, _jobs)
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
        val xsdAttributes = topElement.head.attributes
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

  def getDatabase(domain: Domain)(implicit settings: Settings): Option[String] =
    domain.database.orElse(settings.appConfig.getDefaultDatabase())
  // SL_DATABASE
  // default database
}
