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

import ai.starlake.config.{DatasetArea, Settings, StorageArea}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.{CometObjectMapper, Utils, YamlSerializer}
import better.files.File
import com.databricks.spark.xml.util.XSDToSchema
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.github.ghik.silencer.silent
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Handles access to datasets metadata, eq. domains / types / schemas.
  *
  * @param storage
  *   : Underlying filesystem manager
  */
class SchemaHandler(storage: StorageHandler)(implicit settings: Settings) extends StrictLogging {

  // uses Jackson YAML for parsing, relies on SnakeYAML for low level handling
  @silent val mapper: ObjectMapper with ScalaObjectMapper =
    new CometObjectMapper(new YAMLFactory(), injectables = (classOf[Settings], settings) :: Nil)

  @throws[Exception]
  def checkValidity(reload: Boolean = false): List[String] = {
    val typesValidity = this.types(reload).map(_.checkValidity())
    val domainsValidity = this.domains(reload).map(_.checkValidity(this))
    val domainsVarsValidity = checkDomainsVars()
    val jobsVarsValidity = checkJobsVars()
    val allErrors = typesValidity ++ domainsValidity :+ checkViewsValidity()

    val errs = allErrors.flatMap {
      case Left(values) => values
      case Right(_)     => Nil
    }
    errs ++ domainsVarsValidity ++ jobsVarsValidity
  }

  def checkDomainsVars(): List[String] = {
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
    paths.flatMap(checkVarsAreDefined)
  }

  def fullValidation(config: ValidateConfig = ValidateConfig(reload = false)): Unit =
    Try {
      val errs = checkValidity(config.reload)
      val deserErrors = deserializedDomains.filter { case (path, res) =>
        res.isFailure
      }
      val errorCount = errs.length + deserErrors.length
      val output =
        settings.comet.rootServe.map(rootServe => File(File(rootServe), "validation.log"))
      output.foreach(_.overwrite(""))
      if (errorCount > 0) {
        output.foreach(
          _.appendLine(
            s"START VALIDATION RESULTS: $errorCount errors found"
          )
        )
        logger.error(s"START VALIDATION RESULTS: $errorCount errors found")
        deserErrors.foreach { case (path, err) =>
          output.foreach(_.appendLine(s"${path.toString} could not be deserialized"))
          logger.error(s"${path.toString} could not be deserialized")
        }
        errs.foreach(err => logger.error(err))
        errs.foreach(err => output.foreach(_.appendLine(err)))
        logger.error(s"END VALIDATION RESULTS")
        output.foreach(_.appendLine(s"END VALIDATION RESULTS"))
      }
    } match {
      case Success(_) => // do nothing
      case Failure(e) =>
        e.printStackTrace()
        if (settings.comet.validateOnLoad)
          throw e
    }

  def checkViewsValidity(): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val viewsPath = DatasetArea.views
    val sqlFiles = storage.list(viewsPath, extension = ".sql", recursive = true)
    val duplicates = sqlFiles.groupBy(_.getName).filter { case (name, paths) => paths.length > 1 }
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
      val content = storage.read(assertionsPath).richFormat(activeEnv(), Map.empty)
      logger.info(s"reading content $content")
      mapper
        .readValue(content, classOf[AssertionDefinitions])
        .assertionDefinitions
    } else
      Map.empty[String, AssertionDefinition]
  }

  @throws[Exception]
  def assertions(name: String): Map[String, AssertionDefinition] = {
    val defaultAssertions = loadAssertions("default.comet.yml")
    val assertions = loadAssertions("assertions.comet.yml")
    val resAssertions = loadAssertions(name + ".comet.yml")

    defaultAssertions ++ assertions ++ resAssertions
  }

  private def loadSqlJ2File(sqlFile: Path): (String, String) = {
    val sqlExpr = storage.read(sqlFile)
    val sqlName = sqlFile.getName().dropRight(".sql.j2".length)
    sqlName -> sqlExpr
  }

  private def loadSqlJi2Files(path: Path): Map[String, String] = {
    val sqlFiles = storage.list(path, extension = ".sql.j2", recursive = true)
    sqlFiles.map { sqlFile =>
      loadSqlJ2File(sqlFile)
    }.toMap
  }

  private var viewsMap = loadSqlJi2Files(DatasetArea.views)

  def views(viewName: String): Option[String] = viewsMap.get(viewName)

  private val cometDateVars = {
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

  def activeEnv(reload: Boolean = false) = if (reload) loadActiveEnv() else _activeEnv
  private var _activeEnv = loadActiveEnv()
  @throws[Exception]
  private def loadActiveEnv(): Map[String, String] = {
    def loadEnv(path: Path): Map[String, String] =
      if (storage.exists(path))
        Option(mapper.readValue(storage.read(path), classOf[Env]).env.getOrElse(Map.empty))
          .getOrElse(Map.empty)
      else
        Map.empty
    val globalsCometPath = new Path(DatasetArea.metadata, s"env.comet.yml")
    val envsCometPath = new Path(DatasetArea.metadata, s"env.${settings.comet.env}.comet.yml")
    val globalEnv = {
      loadEnv(globalsCometPath).mapValues(
        _.richFormat(sys.env, cometDateVars)
      ) // will replace with sys.env
    }
    val localEnv =
      loadEnv(envsCometPath).mapValues(_.richFormat(sys.env, globalEnv ++ cometDateVars))
    this._activeEnv = cometDateVars ++ globalEnv ++ localEnv
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

  var deserializedDomains: List[(Path, Try[Domain])] = {
    val paths = storage.list(
      DatasetArea.domains,
      extension = ".yml",
      recursive = true,
      exclude = Some(Pattern.compile("_.*"))
    )

    val domains = paths
      .map { path =>
        YamlSerializer.deserializeDomain(
          storage.read(path).richFormat(activeEnv(), Map.empty),
          path.toString
        )
      }

    paths.zip(domains)
  }

  def domains(reload: Boolean = false) = if (reload) loadDomains() else _domains

  private var _domains = loadDomains()

  /** All defined domains Domains are defined under the "domains" folder in the metadata folder
    */
  @throws[Exception]
  private def loadDomains(): List[Domain] = {
    val (validDomainsFile, invalidDomainsFiles) = deserializedDomains
      .map {
        case (path, Success(domain)) =>
          val folder = path.getParent()
          val schemaRefs = domain.tableRefs
            .getOrElse(Nil)
            .map { ref =>
              if (!ref.startsWith("_"))
                throw new Exception(
                  s"reference to a schema should start with '_' in domain ${domain.name} in $path for schema ref $ref"
                )
              val refFullName =
                if (ref.endsWith(".yml") || ref.endsWith(".yaml")) ref else ref + ".comet.yml"
              val schemaPath = new Path(folder, refFullName)
              YamlSerializer.deserializeSchemas(
                storage.read(schemaPath).richFormat(activeEnv(), Map.empty),
                schemaPath.toString
              )
            }
            .flatMap(_.tables)
          Success(domain.copy(tables = Option(domain.tables).getOrElse(Nil) ::: schemaRefs))
        case (path, Failure(e)) =>
          Utils.logException(logger, e)
          Failure(e)
      }
      .partition(_.isSuccess)

    invalidDomainsFiles.foreach {
      case Failure(err) =>
        logger.error(
          s"There is one or more invalid Yaml files in your domains folder:${err.getMessage}"
        )
        if (settings.comet.validateOnLoad)
          throw err
      case Success(_) => // ignore
    }

    val domains = validDomainsFile
      .collect { case Success(domain) => domain }
      .map(domain => this.fromXSD(domain))

    Utils.duplicates(
      domains.map(_.name),
      s"%s is defined %d times. A domain can only be defined once."
    ) match {
      case Right(_) => domains
      case Left(errors) =>
        errors.foreach(logger.error(_))
        throw new Exception("Duplicated domain name(s)")
    }

    this._domains = Utils.duplicates(
      domains.map(_.resolveDirectory()),
      s"%s is defined %d times. A directory can only appear once in a domain definition file."
    ) match {
      case Right(_) => domains
      case Left(errors) =>
        errors.foreach(logger.error(_))
        throw new Exception("Duplicated domain directory name")
    }
    this._domains
  }

  def checkVarsAreDefined(path: Path): Set[String] = {
    val vars = storage.read(path).extractVars()
    val envVars = activeEnv().keySet
    val undefinedVars = vars.diff(envVars)
    undefinedVars.map(undefVar => s"${path.toString} contains undefined vars: ${undefVar}")

  }

  def loadJobFromFile(path: Path): Try[AutoJobDesc] =
    Try {
      val rootNode = mapper.readTree(storage.read(path).richFormat(activeEnv(), Map.empty))
      val tranformNode = rootNode.path("transform")
      val autojobNode =
        if (tranformNode.isNull() || tranformNode.isMissingNode) {
          logger.warn(
            s"Defining a autojob outside a transform node is now deprecated. Please update definition $path"
          )
          rootNode
        } else
          tranformNode
      val tasksNode = autojobNode.path("tasks").asInstanceOf[ArrayNode]
      for (i <- 0 until tasksNode.size()) {
        val taskNode = tasksNode.get(i).asInstanceOf[ObjectNode]
        YamlSerializer.renameField(taskNode, "dataset", "table")
      }

      // Now load any sql file related to this JOB
      // for file job.comet.yml containing a single unnamed task, we search for job.sql
      // for file job.comet.yml containing multiple named tasks (say task1, task2), we search for job.task1.sql & job.task2.sql
      val jobDesc = mapper.treeToValue(autojobNode, classOf[AutoJobDesc])
      val sqlFilePrefix = path.toString.substring(0, path.toString.length - ".comet.yml".length)
      val tasks = jobDesc.tasks.map { taskDesc =>
        val sqlTaskFile = taskDesc.name match {
          case Some(taskName) => new Path(s"$sqlFilePrefix.$taskName.sql")
          case None           => new Path(s"$sqlFilePrefix.sql")
        }
        val j2TaskFile = taskDesc.name match {
          case Some(taskName) => new Path(s"$sqlFilePrefix.$taskName.sql.j2")
          case None           => new Path(s"$sqlFilePrefix.sql.j2")
        }
        val commonTaskDesc = taskDesc.copy(
          domain = Option(taskDesc.domain).getOrElse("").richFormat(activeEnv(), Map.empty),
          table = taskDesc.table.richFormat(activeEnv(), Map.empty),
          area = taskDesc.area.map(area =>
            StorageArea.fromString(area.value.richFormat(activeEnv(), Map.empty))
          )
        )
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
          .map { taskFile =>
            val sqlTask = SqlTaskExtractor(storage.read(taskFile))
            taskDesc.copy(
              domain = Option(taskDesc.domain).getOrElse("").richFormat(activeEnv(), Map.empty),
              table = taskDesc.table.richFormat(activeEnv(), Map.empty),
              area = taskDesc.area.map(area =>
                StorageArea.fromString(area.value.richFormat(activeEnv(), Map.empty))
              ),
              presql = sqlTask.presql,
              sql = Option(sqlTask.sql),
              postsql = sqlTask.postsql
            )
          }
          .getOrElse(taskDesc)
      /*
          import com.hubspot.jinjava.Jinjava
          import scala.collection.JavaConverters._
          val jinjava = new Jinjava()
          jinjava.setResourceLocator(new JinjaResourceHandler())
          val context = activeEnv.asJava
          commonTaskDesc.copy(
            presql = sqlTask.presql.map(sqls => sqls.map(sql => jinjava.render(sql, context))),
            sql = Option(jinjava.render(sqlTask.sql, context)),
            postsql = sqlTask.postsql.map(sqls => sqls.map(sql => jinjava.render(sql, context)))
          )
       */
      }
      val jobName = finalDomainOrJobName(path, jobDesc.name)
      jobDesc.copy(
        name = jobName,
        tasks = tasks
      )
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        Failure(new Exception(s"Invalid Job file: $path(${exception.getMessage})", exception))
    }

  /** To be deprecated soon
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

  def jobs(reload: Boolean = false): Map[String, AutoJobDesc] = if (reload) loadJobs() else _jobs
  private var _jobs: Map[String, AutoJobDesc] = loadJobs()

  /** All defined jobs Jobs are defined under the "jobs" folder in the metadata folder
    */
  @throws[Exception]
  private def loadJobs(): Map[String, AutoJobDesc] = {
    val jobs = storage.list(DatasetArea.jobs, ".yml", recursive = true)
    val (validJobsFile, invalidJobsFile) =
      jobs
        .map(loadJobFromFile)
        .partition(_.isSuccess)

    invalidJobsFile.foreach {
      case Failure(err) =>
        err.printStackTrace()
        logger.error(
          s"There is one or more invalid Yaml files in your jobs folder:${err.getMessage}"
        )
        if (settings.comet.validateOnLoad)
          throw err
      case Success(_) => // do nothing
    }

    this._jobs = validJobsFile
      .collect { case Success(job) => job }
      .map(job => job.name -> job)
      .toMap
    this._jobs
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
        val xsdAttributes = topElement.head.attributes.map(_.toList).getOrElse(Nil)
        val merged = mergeAttributes(ymlSchema.attributes, xsdAttributes)
        ymlSchema.copy(attributes = merged)
    }
  }

  def mergeAttributes(ymlAttrs: List[Attribute], xsdAttrs: List[Attribute]): List[Attribute] = {
    val ymlTopLevelAttr = Attribute("__dummy", "struct", attributes = Some(ymlAttrs))
    val xsdTopLevelAttr = Attribute("__dummy", "struct", attributes = Some(xsdAttrs))

    val merged = xsdTopLevelAttr.importAttr(ymlTopLevelAttr)
    merged.attributes.getOrElse(Nil)
  }
}
