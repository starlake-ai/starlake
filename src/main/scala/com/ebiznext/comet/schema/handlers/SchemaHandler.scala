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

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.generator.YamlSerializer
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.CometObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

/** Handles access to datasets metadata,  eq. domains / types / schemas.
  *
  * @param storage : Underlying filesystem manager
  */
class SchemaHandler(storage: StorageHandler)(implicit settings: Settings) extends StrictLogging {

  // uses Jackson YAML for parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper with ScalaObjectMapper = {
    val mapper =
      new CometObjectMapper(new YAMLFactory(), injectables = (classOf[Settings], settings) :: Nil)

    mapper
  }

  /** All defined types.
    * Load all default types defined in the file default.comet.yml
    * Types are located in the only file "types.comet.yml"
    * Types redefined in the file "types.comet.yml" supersede the ones in "default.comet.yml"
    */
  lazy val types: List[Type] = {
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

    val defaultTypes = loadTypes("default")
    val types = loadTypes("types")

    val redefinedTypeNames =
      defaultTypes.map(_.name).intersect(types.map(_.name))

    defaultTypes.filter(defaultType => !redefinedTypeNames.contains(defaultType.name)) ++ types
  }

  def assertions(name: String): Map[String, AssertionDefinition] = {
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

    val defaultAssertions = loadAssertions("default.comet.yml")
    val assertions = loadAssertions("assertions.comet.yml")
    val resAssertions = loadAssertions(name + ".comet.yml")

    defaultAssertions ++ assertions ++ resAssertions
  }

  def views(name: String): Views = {
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

    Views.merge(
      ("default.comet.yml" :: "views.comet.yml" :: (name + ".comet.yml") :: Nil).map(loadViews)
    )
  }

  lazy val activeEnv: Map[String, String] = {
    def loadEnv(path: Path) =
      if (storage.exists(path))
        mapper.readValue(storage.read(path), classOf[Env]).env
      else
        Map.empty[String, String]
    val globalsCometPath = new Path(DatasetArea.metadata, s"env.comet.yml")
    val envsCometPath = new Path(DatasetArea.metadata, s"env.${settings.comet.env}.comet.yml")
    loadEnv(globalsCometPath) ++ loadEnv(envsCometPath)
  }

  /** Fnd type by name
    *
    * @param tpe : Type name
    * @return Unique type referenced by this name.
    */
  def getType(tpe: String): Option[Type] = {
    types.find(_.name == tpe)
  }

  /** All defined domains
    * Domains are defined under the "domains" folder in the metadata folder
    */
  lazy val domains: List[Domain] = {
    val (validDomainsFile, invalidDomainsFiles) = storage
      .list(DatasetArea.domains, ".yml", recursive = true)
      .map { path =>
        YamlSerializer.deserializeDomain(storage.read(path))
      }
      .partition(_.isSuccess)
    invalidDomainsFiles.foreach {
      case Failure(err) =>
        logger.warn(
          s"There is one or more invalid Yaml files in your domains folder:${err.getMessage}"
        )
      case Success(_) => // ignore
    }
    validDomainsFile.collect { case Success(domain) => domain }
  }

  def loadJobFromFile(path: Path): Try[AutoJobDesc] = {
    Try {
      val rootNode = mapper.readTree(storage.read(path))
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
          val sqlTask = SqlTask(sqlTaskFile)
          taskDesc.copy(
            presql = sqlTask.presql,
            sql = taskDesc.sql,
            postsql = sqlTask.postsql
          )
        } else {
          taskDesc
        }
      }
      if (path.getName != s"${jobDesc.name}.comet.yml") {
        logger.warn(
          s"Please set the job name to ${path.getName} to reflect the filename. This feature will be deprecated soon"
        )
      }
      jobDesc.copy(tasks = tasks)
    }
  }

  /** All defined jobs
    * Jobs are defined under the "jobs" folder in the metadata folder
    */
  lazy val jobs: Map[String, AutoJobDesc] = {
    val (validJobsFile, invalidJobsFile) = storage
      .list(DatasetArea.jobs, ".yml", recursive = true)
      .map(loadJobFromFile)
      .partition(_.isSuccess)

    invalidJobsFile.foreach {
      case Failure(err) =>
        logger.warn(
          s"There is one or more invalid Yaml files in your jobs folder:${err.getMessage}"
        )
      case Success(_) => // do nothing
    }

    validJobsFile
      .collect { case Success(job) => job }
      .map(job => job.name -> job)
      .toMap
  }

  /** Find domain by name
    *
    * @param name : Domain name
    * @return Unique Domain referenced by this name.
    */
  def getDomain(name: String): Option[Domain] = {
    domains.find(_.name == name)
  }

  /** Return all schemas for a domain
    *
    * @param domain : Domain name
    * @return List of schemas for a domain, empty list if no schema or domain is found
    */
  def getSchemas(domain: String): List[Schema] = {
    getDomain(domain).map(_.schemas).getOrElse(Nil)
  }

  /** Get schema by name for a domain
    *
    * @param domainName : Domain name
    * @param schemaName : Sceham name
    * @return Unique Schema with this name for a domain
    */
  def getSchema(domainName: String, schemaName: String): Option[Schema] = {
    for {
      domain <- getDomain(domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield schema
  }
}
