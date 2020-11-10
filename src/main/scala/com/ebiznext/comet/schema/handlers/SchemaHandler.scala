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
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.CometObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.Try

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
    * Load all default types defined in the file default.yml
    * Types are located in the only file "types.yml"
    * Types redefined in the file "types.yml" supersede the ones in "default.yml"
    */
  lazy val types: List[Type] = {
    def loadTypes(filename: String): List[Type] = {
      val typesPath = new Path(DatasetArea.types, filename)
      if (storage.exists(typesPath))
        mapper.readValue(storage.read(typesPath), classOf[Types]).types
      else
        List.empty[Type]
    }

    val defaultTypes = loadTypes("default.yml")
    val types = loadTypes("types.yml")

    val redefinedTypeNames =
      defaultTypes.map(_.name).intersect(types.map(_.name))

    defaultTypes.filter(defaultType => !redefinedTypeNames.contains(defaultType.name)) ++ types
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
      .list(DatasetArea.domains, ".yml")
      .map(path => Try(mapper.readValue(storage.read(path), classOf[Domain])))
      .partition(_.isSuccess)
    invalidDomainsFiles.map(_.failed.get).foreach { err =>
      logger.warn(
        s"There is one or more invalid Yaml files in your domains folder:${err.getMessage}"
      )
    }
    validDomainsFile.map(_.get)

  }

  /** All defined jobs
    * Jobs are defined under the "jobs" folder in the metadata folder
    */
  lazy val jobs: Map[String, AutoJobDesc] = {
    val (validJobsFile, invalidJobsFile) = storage
      .list(DatasetArea.jobs, ".yml")
      .map(path => Try(mapper.readValue(storage.read(path), classOf[AutoJobDesc])))
      .partition(_.isSuccess)
    invalidJobsFile.map(_.failed.get).foreach { err =>
      logger.warn(s"There is one or more invalid Yaml files in your jobs folder:${err.getMessage}")
    }
    validJobsFile
      .map(_.get)
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
