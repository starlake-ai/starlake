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
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
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
  ): List[String] = {
    def toWarning(warnings: List[String]) = {
      warnings.map("Warning: " + _)
    }

    val typesValidity = this.types(reload).map(_.checkValidity())
    val loadedDomains = this.domains(reload)
    val domainsValidity = loadedDomains.map(_.checkValidity(this, directorySeverity)).map {
      case Left((errors, warnings)) => Left(errors ++ toWarning(warnings))
      case Right(right)             => Right(right)
    }
    val domainsVarsValidity = checkDomainsVars()
    val jobsVarsValidity = toWarning(checkJobsVars()) // job vars may be defined at runtime.
    val allErrors = typesValidity ++ domainsValidity :+ checkViewsValidity()

    val errs = allErrors.flatMap {
      case Left(values) => values
      case Right(_)     => Nil
    }
    errs ++ domainsVarsValidity ++ jobsVarsValidity
  }

  private def checkDomainsVars(): List[String] = {
    val paths = storage.list(
      DatasetArea.domains,
      extension = ".yml",
      recursive = true,
      exclude = Some(Pattern.compile("_.*"))
    )
    paths.flatMap(checkVarsAreDefined)
  }

  def checkJobsVars(): List[String] = {
    val paths = storage.list(DatasetArea.jobs, ".yml", recursive = true)
    paths.flatMap { path =>
      val ymlWarnings = checkVarsAreDefined(path)
      val sqlPath = taskSqlPath(path, "")
      val sqlWarnings = sqlPath.map(checkVarsAreDefined).getOrElse(Nil)
      ymlWarnings ++ sqlWarnings
    }
  }

  def fullValidation(
    config: ValidateConfig = ValidateConfig()
  ): Unit = {
    val validityErrorsAndWarnings =
      checkValidity(directorySeverity = Warning, reload = config.reload)
    val deserErrors = deserializedDomains(DatasetArea.domains)
      .filter { case (path, res) =>
        res.isFailure
      } map { case (path, err) =>
      s"${path.toString} could not be deserialized"
    }

    val allErrorsAndWarnings =
      validityErrorsAndWarnings ++ deserErrors ++ this._domainErrors ++ this._jobErrors
    val (warnings, errors) = allErrorsAndWarnings.partition(_.startsWith("Warning: "))
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
        logger.error(err)
        output.foreach(_.appendLine(err))
      }
      logger.error(s"END VALIDATION RESULTS")
      output.foreach(_.appendLine(s"END VALIDATION RESULTS"))
      if (settings.comet.validateOnLoad)
        throw new Exception(
          s"Validation Failed: $errorCount errors and $warningCount warning found"
        )
    }
  }

  def checkViewsValidity(): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val viewsPath = DatasetArea.views
    val sqlFiles =
      storage.list(viewsPath, extension = ".sql", recursive = true) ++
      storage.list(viewsPath, extension = ".sql.j2", recursive = true)

    val duplicates = sqlFiles.groupBy(_.getName).filter { case (name, paths) => paths.length > 1 }
    // Check Domain name validity
    sqlFiles.foreach { sqlFile =>
      val name = viewName(sqlFile)
      if (!forceViewPrefixRegex.pattern.matcher(name).matches()) {
        errorList += s"View with name $name should respect the pattern ${forceViewPrefixRegex.regex}"
      }
    }

    duplicates.foreach { duplicate =>
      errorList += s"Found duplicate views => $duplicate"
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

  def loadAssertions(filename: String): Map[String, AssertionDefinition] = {
    val assertionsPath = new Path(DatasetArea.assertions, filename)
    logger.info(s"Loading assertions $assertionsPath")
    if (storage.exists(assertionsPath)) {
      val content = Utils
        .parseJinja(storage.read(assertionsPath), activeEnv())
        .richFormat(activeEnv(), Map.empty)
      mapper
        .readValue(content, classOf[AssertionDefinitions])
        .assertionDefinitions
    } else
      Map.empty[String, AssertionDefinition]
  }

  def loadExternalSources(filename: String): List[ExternalProject] = {
    val externalPath = new Path(DatasetArea.external, filename)
    logger.info(s"Loading assertions $externalPath")
    if (storage.exists(externalPath)) {
      val content = Utils
        .parseJinja(storage.read(externalPath), activeEnv())
        .richFormat(activeEnv(), Map.empty)
      mapper
        .readValue(content, classOf[ExternalSource])
        .projects
    } else
      List.empty[ExternalProject]
  }

  @throws[Exception]
  def externalSources(): List[ExternalProject] = loadExternalSources("default.comet.yml")

  @throws[Exception]
  def assertions(name: String): Map[String, AssertionDefinition] = {
    val defaultAssertions = loadAssertions("default.comet.yml")
    val assertions = loadAssertions("assertions.comet.yml")
    val resAssertions = loadAssertions(name + ".comet.yml")
    defaultAssertions ++ assertions ++ resAssertions
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

  private def loadSqlJi2Files(path: Path): Map[String, String] = {
    val allFiles = listSqlj2Files(path)
    allFiles.map { sqlFile =>
      loadSqlJ2File(sqlFile)
    }.toMap
  }

  def views(): Map[String, String] = loadSqlJi2Files(DatasetArea.views)

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

  def activeEnv(reload: Boolean = false): Map[String, String] =
    if (reload) loadActiveEnv() else _activeEnv

  private var _activeEnv = loadActiveEnv()

  @throws[Exception]
  private def loadActiveEnv(): Map[String, String] = {
    def loadEnv(path: Path): Map[String, String] =
      if (storage.exists(path))
        Option(mapper.readValue(storage.read(path), classOf[Env]).env)
          .getOrElse(Map.empty)
      else
        Map.empty
    // We first load all variables defined in the common environment file.
    // variables defined here are default values.
    val globalsCometPath = new Path(DatasetArea.metadata, s"env.comet.yml")
    // The env var COMET_ENV should be set to the profile under wich starlake is run.
    // If no profile is defined, only default values are used.
    val envsCometPath = new Path(DatasetArea.metadata, s"env.${settings.comet.env}.comet.yml")
    // System Env variables may be used as valuesY
    val globalEnv = {
      loadEnv(globalsCometPath).mapValues(
        _.richFormat(sys.env, cometDateVars)
      ) // will replace with sys.env
    }
    // We subsittute values defined in the current profile with variables defined
    // in the default env file
    val localEnv =
      loadEnv(envsCometPath).mapValues(_.richFormat(sys.env, globalEnv ++ cometDateVars))

    // Please note below how profile specific vars override default profile vars.
    this._activeEnv = cometDateVars ++ globalEnv ++ localEnv ++ cliEnv
    this._activeEnv
  }

  /** Fnd type by name
    *
    * @param tpe
    *   : Type name
    * @return
    *   Unique type referenced by this name.
    */
  def getType(tpe: String): Option[Type] = types().find(_.name == tpe)

  def deserializedDomains(domainPath: Path): List[(Path, Try[Domain])] = {
    val paths = storage.list(
      domainPath,
      extension = ".yml",
      recursive = true,
      exclude = Some(Pattern.compile("_.*"))
    )

    val domains = paths
      .map { path =>
        YamlSerializer.deserializeDomain(
          Utils.parseJinja(storage.read(path), activeEnv()),
          path.toString
        )
      }
    paths.zip(domains)
  }

  def domains(reload: Boolean = false): List[Domain] = {
    if (reload)
      loadDomains()
    _domains
  }

  private var (_domainErrors, _domains): (List[String], List[Domain]) = loadDomains()

  def loadDomains(): (List[String], List[Domain]) = {
    loadDomains(DatasetArea.domains)
  }

  def loadExternals(): (List[String], List[Domain]) = {
    loadDomains(DatasetArea.external)
  }

  /** All defined domains Domains are defined under the "domains" folder in the metadata folder
    */
  @throws[Exception]
  private def loadDomains(area: Path): (List[String], List[Domain]) = {
    val (validDomainsFile, invalidDomainsFiles) = deserializedDomains(area)
      .map {
        case (path, Success(domain)) =>
          logger.info(s"Loading domain from $path")
          val folder = path.getParent()
          val tableRefNames = domain.tableRefs match {
            case "*" :: Nil =>
              storage
                .list(folder, extension = ".yml", recursive = true)
                .map(_.getName())
                .filter(_.startsWith("_"))
            case _ =>
              domain.tableRefs.map { ref =>
                if (!ref.startsWith("_"))
                  throw new Exception(
                    s"reference to a schema should start with '_' in domain ${domain.name} in $path for schema ref $ref"
                  )
                if (ref.endsWith(".comet.yml") || ref.endsWith(".yaml")) ref else ref + ".comet.yml"
              }
          }

          val schemaRefs = tableRefNames
            .map { tableRefName =>
              val schemaPath = new Path(folder, tableRefName)
              YamlSerializer.deserializeSchemaRefs(
                Utils
                  .parseJinja(storage.read(schemaPath), activeEnv())
                  .richFormat(activeEnv(), Map.empty),
                schemaPath.toString
              )
            }
            .flatMap(_.tables)
            .map(t => t.copy(metadata = Some(t.mergedMetadata(domain.metadata))))
          logger.info(s"Successfully loaded Domain  in $path")
          Success(domain.copy(tables = Option(domain.tables).getOrElse(Nil) ::: schemaRefs))
        case (path, Failure(e)) =>
          logger.error(s"Failed to load domain in $path")
          Utils.logException(logger, e)
          Failure(e)
      }
      .partition(_.isSuccess)

    val domains = validDomainsFile
      .collect { case Success(domain) => domain }
      .map(domain => this.fromXSD(domain))

    val nameErrors = Utils.duplicates(
      domains.map(_.name),
      s"%s is defined %d times. A domain can only be defined once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val renameErrors = Utils.duplicates(
      domains.map(d => d.rename.getOrElse(d.name)),
      s"renamed domain %s is defined %d times. It can only appear once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val directoryErrors = Utils.duplicates(
      domains.flatMap(_.resolveDirectoryOpt()),
      s"%s is defined %d times. A directory can only appear once in a domain definition file."
    ) match {
      case Right(_) =>
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
    this._domainErrors.foreach(logger.error(_))
    (this._domainErrors, this._domains)
  }

  private def checkVarsAreDefined(path: Path): Set[String] = {
    val content = storage.read(path)
    checkVarsAreDefined(path, content)
  }

  private def checkVarsAreDefined(path: Path, content: String): Set[String] = {
    val vars = content.extractVars()
    val envVars = activeEnv().keySet
    val undefinedVars = vars.diff(envVars)
    undefinedVars.map(undefVar => s"""${path.getName} contains undefined var: ${undefVar}""")
  }

  def loadJobFromFile(jobPath: Path): Try[AutoJobDesc] =
    Try {
      val fileContent = storage.read(jobPath)
      val rootContent = Utils.parseJinja(fileContent, activeEnv())

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
      val tasksNode = autojobNode.path("tasks").asInstanceOf[ArrayNode]
      for (i <- 0 until tasksNode.size()) {
        val taskNode = tasksNode.get(i).asInstanceOf[ObjectNode]
        YamlSerializer.renameField(taskNode, "dataset", "table")
        taskNode.path("sink") match {
          case node if node.isMissingNode => // do nothing
          case sinkNode =>
            sinkNode.path("type") match {
              case node if node.isMissingNode => // do thing
              case node =>
                val textNode = node.asInstanceOf[TextNode]
                val sinkType = textNode.textValue().replaceAll("\"", "").toUpperCase()
                val parent = sinkNode.asInstanceOf[ObjectNode]
                if (sinkType == "DATABRICKS" || sinkType == "HIVE")
                  parent.replace("type", new TextNode("FS"))
                else if (sinkType == "BIGQUERY")
                  parent.replace("type", new TextNode("BQ"))
            }
        }
      }

      // Now load any sql file related to this JOB
      // for file job.comet.yml containing a single unnamed task, we search for job.sql
      // for file job.comet.yml containing multiple named tasks (say task1, task2), we search for job.task1.sql & job.task2.sql
      val jobDesc = mapper.treeToValue(autojobNode, classOf[AutoJobDesc])
      val folder = jobPath.getParent()
      val autoTasksRefs = loadTaskRefs(jobPath, jobDesc, folder)

      logger.info(s"Successfully loaded job  in $jobPath")
      val jobDescWithRefs: AutoJobDesc =
        jobDesc.copy(tasks = Option(jobDesc.tasks).getOrElse(Nil) ::: autoTasksRefs)

      val tasks = jobDescWithRefs.tasks.map { taskDesc =>
        val taskSqlFile: Option[Path] = taskSqlPath(jobPath, taskDesc.name)

        taskSqlFile
          .map { taskFile =>
            val sqlTask = SqlTaskExtractor(storage.read(taskFile))
            taskDesc.copy(
              domain = Option(taskDesc.domain).getOrElse(""),
              table = taskDesc.table,
              presql = sqlTask.presql,
              sql = Option(sqlTask.sql),
              postsql = sqlTask.postsql
            )
          }
          .getOrElse(taskDesc)
      }
      val jobName = finalDomainOrJobName(jobPath, jobDesc.name)
      val tasksWithNoSQL = tasks.filter(_.sql.isEmpty)
      assert(
        tasksWithNoSQL.isEmpty,
        s"Tasks ${tasksWithNoSQL.map(_.name).mkString(",")} in job ${jobDesc.name} in path $jobPath need to define a query either through sql attribute or a sql file."
      )
      jobDesc.copy(
        name = jobName,
        tasks = tasks,
        taskRefs = Nil
      )
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        Failure(new Exception(s"Invalid Job file: $jobPath(${exception.getMessage})", exception))
    }

  private def loadTaskRefs(path: Path, jobDesc: AutoJobDesc, folder: Path): List[AutoTaskDesc] = {
    val autoTasksRefNames = jobDesc.taskRefs match {
      case "*" :: Nil =>
        storage
          .list(folder, extension = ".comet.yml", recursive = true)
          .map(_.getName())
          .filter(_.startsWith("_"))
          .map { ref =>
            val taskName = ref.substring(1, ref.length - ".comet.yml".length)
            (taskName, ref)
          }
      case _ =>
        jobDesc.taskRefs.map { ref =>
          if (!ref.startsWith("_"))
            throw new Exception(
              s"reference to a job should start with '_' in task $ref in $path for job ${jobDesc.name}"
            )
          if (ref.endsWith(".comet.yml")) {
            val taskName = ref.substring(1, ref.length - ".comet.yml".length)
            (taskName, ref)

          } else {
            (ref.substring(1), ref + ".comet.yml")
          }
        }
    }

    val autoTasksRefs = autoTasksRefNames.map { case (taskName, autoTasksRefName) =>
      val taskPath = new Path(folder, autoTasksRefName)
      YamlSerializer
        .deserializeTask(
          Utils
            .parseJinja(storage.read(taskPath), activeEnv())
            .richFormat(activeEnv(), Map.empty)
        )
        .copy(name = taskName)
    }
    autoTasksRefs
  }

  private def taskSqlPath(path: Path, taskName: String): Option[Path] = {
    val sqlFilePrefix = path.toString.substring(0, path.toString.length - ".comet.yml".length)
    val sqlTaskFile = taskName.nonEmpty match {
      case true  => new Path(s"$sqlFilePrefix.$taskName.sql")
      case false => new Path(s"$sqlFilePrefix.sql")
    }
    val j2TaskFile = taskName.nonEmpty match {
      case true  => new Path(s"$sqlFilePrefix.$taskName.sql.j2")
      case false => new Path(s"$sqlFilePrefix.sql.j2")
    }
    val taskFile = (storage.exists(sqlTaskFile), storage.exists(j2TaskFile)) match {
      case (true, true) =>
        Some(j2TaskFile)
      case (true, false) =>
        Some(sqlTaskFile)
      case (false, true) =>
        Some(j2TaskFile)
      case (false, false) =>
        None
    }
    taskFile
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
          Utils.parseJinja(storage.read(path), activeEnv()),
          path.toString
        )
      }
    paths.zip(jobs)
  }

  def jobs(reload: Boolean = false): Map[String, AutoJobDesc] = {
    if (reload) loadJobs()
    _jobs
  }

  def iamPolicyTags(): Option[IamPolicyTags] = {
    val path = DatasetArea.iamPolicyTags()
    if (storage.exists(path))
      Some(YamlSerializer.deserializeIamPolicyTags(storage.read(path)))
    else
      None
  }

  def job(jobName: String): Option[AutoJobDesc] = jobs().get(jobName)

  private var (_jobErrors, _jobs): (List[String], Map[String, AutoJobDesc]) = loadJobs()

  /** All defined jobs Jobs are defined under the "jobs" folder in the metadata folder
    */
  @throws[Exception]
  private def loadJobs(): (List[String], Map[String, AutoJobDesc]) = {
    val jobs = storage.list(
      DatasetArea.jobs,
      ".yml",
      recursive = true,
      exclude = Some(Pattern.compile("_.*"))
    )
    val filenameErrors = Utils.duplicates(
      jobs.map(_.getName()),
      s"%s is defined %d times. A job can only be defined once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val (validJobsFile, invalidJobsFile) =
      jobs
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
      validJobs.map(_.name),
      s"%s is defined %d times. A job can only be defined once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }

    val namePatternErrors = validJobs.map(_.name).flatMap { name =>
      if (!forceJobPrefixRegex.pattern.matcher(name).matches())
        Some(s"View with name $name should respect the pattern ${forceJobPrefixRegex.regex}")
      else
        None
    }

    val taskNamePatternErrors =
      validJobs.flatMap(_.tasks.filter(_.name.nonEmpty).map(_.name)).flatMap { name =>
        if (!forceTaskPrefixRegex.pattern.matcher(name).matches())
          Some(s"View with name $name should respect the pattern ${forceTaskPrefixRegex.regex}")
        else
          None
      }

    this._jobs = validJobs.map(job => job.name -> job).toMap
    this._jobErrors = filenameErrors ++ nameErrors ++ namePatternErrors ++ taskNamePatternErrors
    (_jobErrors, _jobs)
  }

  /** Find domain by name
    *
    * @param name
    *   : Domain name
    * @return
    *   Unique Domain referenced by this name.
    */
  def getDomain(name: String): Option[Domain] = domains().find(_.name == name)

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
        val merged = mergeAttributes(ymlSchema.attributes, xsdAttributes)
        ymlSchema.copy(attributes = merged)
    }
  }

  def mergeAttributes(ymlAttrs: List[Attribute], xsdAttrs: List[Attribute]): List[Attribute] = {
    val ymlTopLevelAttr = Attribute("__dummy", "struct", attributes = ymlAttrs)
    val xsdTopLevelAttr = Attribute("__dummy", "struct", attributes = xsdAttrs)

    val merged = xsdTopLevelAttr.importAttr(ymlTopLevelAttr)
    merged.attributes
  }
}
