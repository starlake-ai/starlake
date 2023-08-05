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

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.{CometObjectMapper, Utils, YamlSerializer}
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

  private val forceViewPrefixRegex: Regex = settings.comet.forceViewPattern.r
  private val forceJobPrefixRegex: Regex = settings.comet.forceJobPattern.r
  private val forceTaskPrefixRegex: Regex = settings.comet.forceTablePattern.r

  // uses Jackson YAML for parsing, relies on SnakeYAML for low level handling
  @nowarn val mapper: ObjectMapper with ScalaObjectMapper =
    new CometObjectMapper(new YAMLFactory(), injectables = (classOf[Settings], settings) :: Nil)

  @throws[Exception]
  private def checkValidity(
    directorySeverity: Severity,
    reload: Boolean = false
  )(implicit storage: StorageHandler): List[ValidationMessage] = {

    val domainStructureValidity = Domain.checkFilenamesValidity()(storage, settings)
    val typesValidity = this.types(reload).map(_.checkValidity())
    val loadedDomains = this.domains(reload)
    val loadedDomainsValidity = loadedDomains.map(_.checkValidity(this, directorySeverity))
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
      extension = ".yml",
      recursive = true,
      exclude = Some(Pattern.compile("_.*"))
    )
    paths.flatMap(checkVarsAreDefined)
  }

  def checkJobsVars(): List[ValidationMessage] = {
    val paths = storage.list(DatasetArea.transform, ".yml", recursive = true)
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

  def fullValidation(
    config: ValidateConfig = ValidateConfig()
  ): Unit = {
    val validityErrorsAndWarnings =
      checkValidity(directorySeverity = Warning, reload = config.reload)(storage)
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
      validityErrorsAndWarnings ++ deserErrors ++ this._domainErrors ++ this._jobErrors
    val (warnings, errors) = allErrorsAndWarnings.partition(_.severity == Warning)
    val errorCount = errors.length
    val warningCount = warnings.length

    val output =
      settings.comet.rootServe.map(rootServe => File(File(rootServe), "validation.log"))
    output.foreach(_.overwrite(""))

    if (errorCount + warningCount > 0) {
      output.foreach(
        _.appendLine(
          s"START VALIDATION RESULTS: $errorCount errors and $warningCount found"
        )
      )
      logger.error(s"START VALIDATION RESULTS: $errorCount errors  and $warningCount found")
      allErrorsAndWarnings.foreach { err =>
        logger.error(err.message)
        output.foreach(_.appendLine(err.message))
      }
      logger.error(s"END VALIDATION RESULTS")
      output.foreach(_.appendLine(s"END VALIDATION RESULTS"))
      if (settings.comet.validateOnLoad)
        throw new Exception(
          s"Validation Failed: $errorCount errors and $warningCount warning found"
        )
    }
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
    val typesCometPath = new Path(DatasetArea.types, filename + ".comet.yml")
    if (storage.exists(typesCometPath))
      mapper.readValue(storage.read(typesCometPath), classOf[Types]).types
    else if (storage.exists(deprecatedTypesPath))
      mapper.readValue(storage.read(deprecatedTypesPath), classOf[Types]).types
    else
      List.empty[Type]
  }

  def types(reload: Boolean = false): List[Type] = if (reload) loadTypes() else _types
  var _types: List[Type] = loadTypes()

  /** All defined types. Load all default types defined in the file default.comet.yml Types are
    * located in the only file "types.comet.yml" Types redefined in the file "types.comet.yml"
    * supersede the ones in "default.comet.yml"
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

  def loadExternalSources(filename: String): List[ExternalProject] = {
    val externalPath = new Path(DatasetArea.external, filename)
    logger.info(s"Loading external $externalPath")
    if (storage.exists(externalPath)) {
      val content = Utils
        .parseJinja(storage.read(externalPath), activeEnvVars())
      mapper
        .readValue(content, classOf[ExternalSource])
        .projects
    } else
      List.empty[ExternalProject]
  }

  @throws[Exception]
  def externalSources(): List[ExternalProject] = loadExternalSources("default.comet.yml")

  @throws[Exception]
  def expectations(name: String): Map[String, ExpectationDefinition] = {
    val defaultExpectations = loadExpectations("default.comet.yml")
    val expectations = loadExpectations("expectations.comet.yml")
    val resExpectations = loadExpectations(name + ".comet.yml")
    defaultExpectations ++ expectations ++ resExpectations
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
      else
        None

    // We first load all variables defined in the common environment file.
    // variables defined here are default values.
    val globalsCometPath = new Path(DatasetArea.metadata, s"env.comet.yml")
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
      .getOrElse(settings.comet.env)
    // The env var SL_ENV should be set to the profile under wich starlake is run.
    // If no profile is defined, only default values are used.
    val envsCometPath = new Path(DatasetArea.metadata, s"env.$activeEnvName.comet.yml")

    // We subsittute values defined in the current profile with variables defined
    // in the default env file
    val localEnvVars =
      loadEnv(envsCometPath)
        .map(_.env)
        .getOrElse(Map.empty)
        .mapValues(_.richFormat(sys.env, globalEnvVars ++ cometDateVars))

    // Please note below how profile specific vars override default profile vars.
    val activeEnvVars = sys.env ++ cometDateVars ++ globalEnvVars ++ localEnvVars ++ cliEnv

    this._activeEnvVars = activeEnvVars
    this._activeEnvVars
  }

  @throws[Exception]
  private def loadRefs(): Refs = {
    val refsPath = new Path(DatasetArea.metadata, "refs.comet.yml")
    val refs = if (storage.exists(refsPath)) {
      val rawContent = storage.read(refsPath)
      val content = Utils.parseJinja(rawContent, activeEnvVars())
      YamlSerializer.mapper.readValue(content, classOf[Refs])
    } else
      Refs(Nil)
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

  def deserializedDomains(domainPath: Path, raw: Boolean = false): List[(Path, Try[Domain])] = {
    // we load only files starting with _config.
    val paths = storage.list(
      domainPath,
      extension = ".yml",
      recursive = true,
      exclude = Some(Pattern.compile("^(?!_config\\.).*$"))
    )

    val domains = paths
      .map { path =>
        YamlSerializer
          .deserializeDomain(
            if (raw) storage.read(path) else Utils.parseJinja(storage.read(path), activeEnvVars()),
            path.toString
          )
          // grants list can be set a comma separated list in YAML , and transformed to a list while parsing
          .map { domain =>
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
                  .copy(emptyIsNull = metadata.emptyIsNull.orElse(Some(settings.comet.emptyIsNull)))

                // set domain name
                val domainName = if (domain.name == "") path.getParent().getName() else domain.name
                domain.copy(name = domainName, tables = tables, metadata = Some(enrichedMetadata))
              }
            }
          }
      }
    paths.zip(domains)
  }

  def domains(reload: Boolean = false, raw: Boolean = false): List[Domain] = {
    if (reload || raw) { // raw is used only for special use cases so we force it to reload
      val (_, domains) = loadDomains(raw)
      domains
    } else {
      _domains
    }
  }

  private var (_domainErrors, _domains): (List[ValidationMessage], List[Domain]) = loadDomains()

  def loadDomains(raw: Boolean = false): (List[ValidationMessage], List[Domain]) = {
    loadDomainsFromArea(DatasetArea.load, raw)
  }

  def loadExternals(): (List[ValidationMessage], List[Domain]) = {
    loadDomainsFromArea(DatasetArea.external)
  }

  def deserializedDagGenerationConfig(dagPath: Path): Try[DagGenerationConfig] = {
    YamlSerializer.deserializeDagGenerationConfig(
      Utils.parseJinjaTpl(storage.read(dagPath), activeEnvVars()),
      dagPath.toString
    )
  }

  /** Global dag generation config can only be defined in "dags" folder in the metadata folder.
    * Override of dag generation config can be done inside domain config file at domain or table
    * level.
    */
  def loadDagGenerationConfig(dagsAreaPath: Path): Try[DagGenerationConfig] = {
    val dagConfigPath = new Path(dagsAreaPath, "default.comet.yml")
    if (storage.exists(dagConfigPath)) {
      deserializedDagGenerationConfig(dagConfigPath)
    } else {
      logger.info("No dags config provided. Use only configuration defined in domain config files.")
      Success(DagGenerationConfig(None, None, None, None, None))
    }
  }

  /** All defined domains Domains are defined under the "domains" folder in the metadata folder
    */
  @throws[Exception]
  private def loadDomainsFromArea(
    area: Path,
    raw: Boolean = false
  ): (List[ValidationMessage], List[Domain]) = {
    val (validDomainsFile, invalidDomainsFiles) = loadFullDomains(area, raw)

    val domains = validDomainsFile
      .collect { case Success(domain) => domain }
      .map(domain => this.fromXSD(domain))

    val nameErrors = Utils.duplicates(
      "Domain name",
      domains.map(_.name),
      s"%s is defined %d times. A domain can only be defined once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val renameErrors = Utils.duplicates(
      "Domain rename",
      domains.map(d => d.rename.getOrElse(d.name)),
      s"renamed domain %s is defined %d times. It can only appear once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val directoryErrors = Utils.duplicates(
      "Domain directory",
      domains.flatMap(_.resolveDirectoryOpt()),
      s"%s is defined %d times. A directory can only appear once in a domain definition file."
    ) match {
      case Right(_) =>
        if (!raw)
          this._domains = domains
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
    (this._domainErrors, domains)
  }

  private def loadFullDomains(area: Path, raw: Boolean): (List[Try[Domain]], List[Try[Domain]]) = {
    val (validDomainsFile, invalidDomainsFiles) = deserializedDomains(area, raw)
      .map {
        case (path, Success(domain)) =>
          logger.info(s"Loading domain from $path")
          val folder = path.getParent()
          val tableRefNames =
            storage
              .list(folder, extension = ".comet.yml", recursive = true)
              .map(_.getName())
              .filter(!_.startsWith("_config."))

          val schemaRefs = tableRefNames
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

  def loadJobFromFile(jobPath: Path): Try[AutoJobDesc] =
    Try {

      val jobDesc = loadJobDesc(jobPath)

      // Load task refs and inject them in the job
      val folder = jobPath.getParent()

      val autoTasksRefs = loadTaskRefs(jobPath, jobDesc, folder)

      logger.info(s"Successfully loaded job  in $jobPath")
      val jobDescWithTaskRefs: AutoJobDesc =
        jobDesc.copy(tasks = Option(jobDesc.tasks).getOrElse(Nil) ::: autoTasksRefs)

      // set task name / domain / table and load sql/py file if any
      val tasks = jobDescWithTaskRefs.tasks.map { taskDesc =>
        val jobFilename = jobPath.getName()
        val jobParentPath = jobPath.getParent()
        val tableNameFromFilename =
          if (taskDesc.name.isEmpty) jobFilename.substring(0, jobFilename.indexOf(".comet.yml"))
          else taskDesc.name
        val domainNameFromFoldername = {
          val parentName = jobParentPath.getName()
          val grandParentName = jobParentPath.getParent().getName()
          if (grandParentName == "metadata" && parentName == "jobs")
            settings.comet.domain
          else
            parentName
        }

        val tableName = if (taskDesc.table.isEmpty) tableNameFromFilename else taskDesc.table
        // Domain name may be set explicitly or else we use the folder name
        val domainName =
          if (taskDesc.domain.isEmpty) domainNameFromFoldername else taskDesc.domain

        val taskName =
          if (taskDesc.name.nonEmpty)
            taskDesc.name
          else
            tableName

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
          taskCommandPath(jobPath.getParent(), taskWithName._filenamePrefix)

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
      val jobName = finalDomainOrJobName(jobPath, jobDesc.name)
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
        Failure(new Exception(s"Invalid Job file: $jobPath(${exception.getMessage})", exception))
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

  private def loadTaskRefs(path: Path, jobDesc: AutoJobDesc, folder: Path): List[AutoTaskDesc] = {
    // List[(prefix, filename, extension)]
    val allFiles =
      storage
        .list(folder, recursive = true)
        .map(_.getName())
        .filter(name => !name.startsWith("_config."))
        .flatMap { filename =>
          // improve the code below to handle more than one extension
          List("comet.yml", "sql", "sql.j2", "py")
            .find(ext => filename.endsWith(s".$ext"))
            .map { ext =>
              val taskName = filename.substring(0, filename.length - s".$ext".length)
              (taskName, filename, ext)
            }
        }
    val ymlFiles = allFiles.filter(_._3 == "comet.yml")
    val sqlPyFiles = allFiles.filter(x =>
      (x._3 == "sql" || x._3 == "sql.j2" || x._3 == "py")
      && !ymlFiles.exists(_._1 == x._1)
      && !jobDesc.tasks.exists(_.name == x._1)
    )
    val autoTasksRefNames: List[(String, String, String)] = ymlFiles ++ sqlPyFiles
    val autoTasksRefs = autoTasksRefNames.map { case (taskFilePrefix, taskFilename, extension) =>
      extension match {
        case "comet.yml" =>
          val taskPath = new Path(folder, taskFilename)
          val taskNode = loadTaskRefNode(
            Utils
              .parseJinja(storage.read(taskPath), activeEnvVars())
          )
          YamlSerializer.upgradeTaskNode(taskNode)
          val taskDesc = YamlSerializer.deserializeTaskNode(taskNode).copy(name = taskFilePrefix)
          val taskName = if (taskDesc.name.isEmpty) taskDesc.name else taskFilePrefix
          taskDesc.copy(_filenamePrefix = taskFilePrefix, name = taskName)
        case _ =>
          AutoTaskDesc(
            name = taskFilePrefix,
            sql = None,
            database = None,
            domain = "",
            table = "",
            write = Some(WriteMode.OVERWRITE),
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

  /** To be deprecated soon
    *
    * @param path
    *   : JOb path
    * @param jobDesc
    *   : job desc
    * @return
    */
  private def finalDomainOrJobName(path: Path, name: String) =
    if (path.getName != s"$name.comet.yml") {
      val newJobName = path.getName.substring(0, path.getName.length - ".comet.yml".length)
      if (name.nonEmpty)
        logger.error(
          s"deprecated: Please set the job name of ${path.getName} to reflect the filename. Job renamed to $newJobName. This feature will be removed soon"
        )
      newJobName
    } else {
      name
    }

  def deserializedJobs(jobPath: Path): List[(Path, Try[AutoJobDesc])] = {
    val paths = storage.list(
      jobPath,
      extension = ".yml",
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
    val configs = storage.list(
      DatasetArea.transform,
      ".comet.yml",
      recursive = true,
      exclude = Some(Pattern.compile("^(?!_config\\.).*$"))
    )
    val (validJobsFile, invalidJobsFile) =
      configs
        .map { path =>
          val result = loadJobFromFile(path)
          result match {
            case Success(_) =>
              logger.info(s"Successfully loaded Job $path")
            case Failure(e) =>
              logger.error(s"Failed to load Job file $path")
              e.printStackTrace()
          }
          result
        }
        .partition(_.isSuccess)

    val validJobs = validJobsFile
      .collect { case Success(job) => job }

    val nameErrors = Utils.duplicates(
      "Job name",
      validJobs.filter(_.name.nonEmpty).map(_.name),
      s"%s is defined %d times. A job can only be defined once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val namePatternErrors = validJobs.filter(_.name.nonEmpty).map(_.name).flatMap { name =>
      if (!forceJobPrefixRegex.pattern.matcher(name).matches())
        Some(
          ValidationMessage(
            Error,
            "View",
            s"View with name $name should respect the pattern ${forceJobPrefixRegex.regex}"
          )
        )
      else
        None
    }

    val taskNamePatternErrors =
      validJobs.flatMap(_.tasks.filter(_.name.nonEmpty).map(_.name)).flatMap { name =>
        if (!forceTaskPrefixRegex.pattern.matcher(name).matches())
          Some(
            ValidationMessage(
              Error,
              "View",
              s"View with name $name should respect the pattern ${forceTaskPrefixRegex.regex}"
            )
          )
        else
          None
      }
    this._jobs = validJobs
    this._jobErrors = nameErrors ++ namePatternErrors ++ taskNamePatternErrors
    (_jobErrors, _jobs)
  }

  /** Find domain by name
    *
    * @param name
    *   : Domain name
    * @return
    *   Unique Domain referenced by this name.
    */
  def getDomain(name: String, raw: Boolean = false): Option[Domain] =
    domains(raw = raw).find(_.name == name)

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
    domain.database.orElse(settings.comet.getDefaultDatabase())
  // SL_DATABASE
  // default database
}
