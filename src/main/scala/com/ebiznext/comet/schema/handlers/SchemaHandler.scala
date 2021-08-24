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

package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.config.{DatasetArea, Settings, StorageArea}
import com.ebiznext.comet.schema.generator.YamlSerializer
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.{CometObjectMapper, Utils}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}
import com.ebiznext.comet.utils.Formatter._

import java.util.regex.Pattern

/** Handles access to datasets metadata, eq. domains / types / schemas.
  *
  * @param storage
  *   : Underlying filesystem manager
  */
class SchemaHandler(storage: StorageHandler)(implicit settings: Settings) extends StrictLogging {

  // uses Jackson YAML for parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper with ScalaObjectMapper = {
    val mapper =
      new CometObjectMapper(new YAMLFactory(), injectables = (classOf[Settings], settings) :: Nil)

    mapper
  }

  @throws[Exception]
  def checkValidity(): Unit = {
    val typesValidity = this.types.map(_.checkValidity())
    val domainsValidty = this.domains.map(_.checkValidity(this))
    this.activeEnv
    this.jobs
    val allErrors = typesValidity ++ domainsValidty
    val errs = allErrors
      .filter(_.isLeft)
      .flatMap(_.left.get)
    errs match {
      case Nil =>
      case _ =>
        errs.foreach(err => logger.error(err))
        throw new Exception("Invalid YML file(s) found. See errors above.")
    }
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

  /** All defined types. Load all default types defined in the file default.comet.yml Types are
    * located in the only file "types.comet.yml" Types redefined in the file "types.comet.yml"
    * supersede the ones in "default.comet.yml"
    */
  @throws[Exception]
  lazy val types: List[Type] = {
    val defaultTypes = loadTypes("default")
    val types = loadTypes("types")

    val redefinedTypeNames =
      defaultTypes.map(_.name).intersect(types.map(_.name))

    defaultTypes.filter(defaultType => !redefinedTypeNames.contains(defaultType.name)) ++ types
  }

  def loadAssertions(filename: String): Map[String, AssertionDefinition] = {
    val assertionsPath = new Path(DatasetArea.assertions, filename)
    logger.info(s"Loading assertions $assertionsPath")
    if (storage.exists(assertionsPath)) {
      val content = storage.read(assertionsPath)
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

  def loadViews(path: String): Views = {
    val viewsPath = DatasetArea.views(path)
    if (storage.exists(viewsPath)) {
      val rootNode = mapper.readTree(storage.read(viewsPath))
      if (rootNode.path("views").isMissingNode)
        throw new Exception(s"Root node views missing in file $path")
      mapper.treeToValue(rootNode, classOf[Views])
    } else {
      Views()
    }
  }

  def views(name: String): Views = {

    Views.merge(
      ("default.comet.yml" :: "views.comet.yml" :: (name + ".comet.yml") :: Nil).map(loadViews)
    )
  }

  @throws[Exception]
  lazy val activeEnv: Map[String, String] = {
    def loadEnv(path: Path): Map[String, String] =
      if (storage.exists(path))
        mapper.readValue(storage.read(path), classOf[Env]).env
      else
        Map.empty[String, String]
    val globalsCometPath = new Path(DatasetArea.metadata, s"env.comet.yml")
    val envsCometPath = new Path(DatasetArea.metadata, s"env.${settings.comet.env}.comet.yml")
    val globalEnv = loadEnv(globalsCometPath)
    val localEnv = loadEnv(envsCometPath).mapValues(_.richFormat(globalEnv))
    globalEnv ++ localEnv
  }

  /** Fnd type by name
    *
    * @param tpe
    *   : Type name
    * @return
    *   Unique type referenced by this name.
    */
  def getType(tpe: String): Option[Type] = {
    types.find(_.name == tpe)
  }

  /** All defined domains Domains are defined under the "domains" folder in the metadata folder
    */
  @throws[Exception]
  lazy val domains: List[Domain] = {
    val (validDomainsFile, invalidDomainsFiles) = storage
      .list(
        DatasetArea.domains,
        extension = ".yml",
        recursive = true,
        exclude = Some(Pattern.compile("_.*"))
      )
      .map { path =>
        val domain =
          YamlSerializer.deserializeDomain(storage.read(path).richFormat(activeEnv), path.toString)
        domain match {
          case Success(domain) =>
            val folder = path.getParent()
            val schemaRefs = domain.schemaRefs
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
                  storage.read(schemaPath).richFormat(activeEnv),
                  schemaPath.toString
                )
              }
              .flatMap(_.schemas)
            Success(domain.copy(schemas = Option(domain.schemas).getOrElse(Nil) ::: schemaRefs))
          case Failure(e) =>
            Failure(e)
        }
      }
      .partition(_.isSuccess)

    invalidDomainsFiles.foreach {
      case Failure(err) =>
        logger.error(
          s"There is one or more invalid Yaml files in your domains folder:${err.getMessage}"
        )
        throw err
      case Success(_) => // ignore
    }

    val domains = validDomainsFile.collect { case Success(domain) => domain }

    Utils.duplicates(
      domains.map(_.name),
      s"%s is defined %d times. A domain can only be defined once."
    ) match {
      case Right(_) => domains
      case Left(errors) =>
        errors.foreach(logger.error(_))
        throw new Exception("Duplicated domain name(s)")
    }

    Utils.duplicates(
      domains.map(_.directory),
      s"%s is defined %d times. A directory can only appear once in a domain definition file."
    ) match {
      case Right(_) => domains
      case Left(errors) =>
        errors.foreach(logger.error(_))
        throw new Exception("Duplicated domain directory name")
    }

  }

  def loadJobFromFile(path: Path): Try[AutoJobDesc] = {
    Try {
      val rootNode = mapper.readTree(storage.read(path).richFormat(activeEnv))
      val tranformNode = rootNode.path("transform")
      val autojobNode =
        if (tranformNode.isNull() || tranformNode.isMissingNode) {
          logger.warn(
            s"Defining a autojob outside a transform node is now deprecated. Please update definition $path"
          )
          rootNode
        } else
          tranformNode
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
        if (storage.exists(sqlTaskFile)) {
          val sqlTask = SqlTask(storage.read(sqlTaskFile))
          taskDesc.copy(
            presql = sqlTask.presql,
            sql = Option(sqlTask.sql),
            postsql = sqlTask.postsql,
            domain = taskDesc.domain.richFormat(activeEnv),
            dataset = taskDesc.dataset.richFormat(activeEnv),
            area =
              taskDesc.area.map(area => StorageArea.fromString(area.value.richFormat(activeEnv)))
          )
        } else {
          taskDesc
        }
      }
      val jobName = finalDomainOrJobName(path, jobDesc.name)
      jobDesc.copy(
        name = jobName,
        tasks = tasks
      )
    }
  }

  /** To be deprecated soon
    * @param path
    *   : JOb path
    * @param jobDesc
    *   : job desc
    * @return
    */
  private def finalDomainOrJobName(path: Path, name: String) = {
    if (path.getName != s"$name.comet.yml") {
      val newJobName = path.getName.substring(0, path.getName.length - ".comet.yml".length)
      logger.error(
        s"deprecated: Please set the job name of ${path.getName} to reflect the filename. Job renamed to $newJobName. This feature will be removed soon"
      )
      newJobName
    } else {
      name
    }
  }

  /** All defined jobs Jobs are defined under the "jobs" folder in the metadata folder
    */
  @throws[Exception]
  lazy val jobs: Map[String, AutoJobDesc] = {
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
        throw err
      case Success(_) => // do nothing
    }

    validJobsFile
      .collect { case Success(job) => job }
      .map(job => job.name -> job)
      .toMap
  }

  /** Find domain by name
    *
    * @param name
    *   : Domain name
    * @return
    *   Unique Domain referenced by this name.
    */
  def getDomain(name: String): Option[Domain] = {
    domains.find(_.name == name)
  }

  /** Return all schemas for a domain
    *
    * @param domain
    *   : Domain name
    * @return
    *   List of schemas for a domain, empty list if no schema or domain is found
    */
  def getSchemas(domain: String): List[Schema] = {
    getDomain(domain).map(_.schemas).getOrElse(Nil)
  }

  /** Get schema by name for a domain
    *
    * @param domainName
    *   : Domain name
    * @param schemaName
    *   : Sceham name
    * @return
    *   Unique Schema with this name for a domain
    */
  def getSchema(domainName: String, schemaName: String): Option[Schema] = {
    for {
      domain <- getDomain(domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield schema
  }
}
