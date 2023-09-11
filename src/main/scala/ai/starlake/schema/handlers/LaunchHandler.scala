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

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryLoadCliConfig
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.sink.jdbc.JdbcConnectionLoadConfig
import ai.starlake.schema.model.{Domain, Schema}
import ai.starlake.utils.{JobResult, Utils}
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.Try

/** Interface required for any cron job launcher
  */
trait LaunchHandler {

  /** Submit to the cron manager a single file for ingestion
    *
    * @param domain
    *   : Domain to which belong this dataset
    * @param schema
    *   : Schema of the dataset
    * @param path
    *   : absolute path where the source dataset (JSON / CSV / ...) is located
    * @return
    *   success / failure
    */
  def ingest(
    workflow: IngestionWorkflow,
    domain: Domain,
    schema: Schema,
    path: Path,
    options: Map[String, String]
  )(implicit
    settings: Settings
  ): Try[JobResult] =
    ingest(workflow, domain, schema, path :: Nil, options)

  /** Submit to the cron manager multiple files for ingestion. All the files should have the schema
    * schema and belong to the same domain.
    *
    * @param domain
    *   : Domain to which belong this dataset
    * @param schema
    *   : Schema of the dataset
    * @param paths
    *   : absolute paths where the source datasets (JSON / CSV / ...) are located
    * @return
    *   success / failure
    */
  def ingest(
    workflow: IngestionWorkflow,
    domain: Domain,
    schema: Schema,
    paths: List[Path],
    options: Map[String, String]
  )(implicit settings: Settings): Try[JobResult]

  /** Index into elasticsearch
    *
    * @param config
    */
  def esLoad(workflow: IngestionWorkflow, config: ESLoadConfig)(implicit
    settings: Settings
  ): Boolean

  /** Load to BigQuery
    *
    * @param config
    */
  def bqload(workflow: IngestionWorkflow, config: BigQueryLoadCliConfig)(implicit
    settings: Settings
  ): Boolean

  /** Load to JDBC Database
    *
    * @param config
    */
  def jdbcload(workflow: IngestionWorkflow, config: JdbcConnectionLoadConfig)(implicit
    settings: Settings
  ): Boolean
}

/** Simple Launcher will directly invoke the ingestion method wityhout using a cron manager. This is
  * userfull for testing purpose
  */
class SimpleLauncher extends LaunchHandler with StrictLogging {

  /** call directly the main assembly with the "ingest" parameter
    *
    * @param domain
    *   : Domain to which belong this dataset
    * @param schema
    *   : Schema of the dataset
    * @param paths
    *   : absolute paths where the source datasets (JSON / CSV / ...) are located
    * @return
    *   success / failure
    */
  override def ingest(
    workflow: IngestionWorkflow,
    domain: Domain,
    schema: Schema,
    paths: List[Path],
    options: Map[String, String]
  )(implicit settings: Settings): Try[JobResult] = {
    logger.info(s"Launch Ingestion: ${domain.name} ${schema.name} $paths ")
    workflow.ingest(domain, schema, paths, options)
  }

  /** Index into elasticsearch
    *
    * @param config
    */
  override def esLoad(workflow: IngestionWorkflow, config: ESLoadConfig)(implicit
    settings: Settings
  ): Boolean = {
    logger.info(s"Launch index: ${config}")
    val result = workflow.esLoad(config)
    Utils.logFailure(result, logger)
    result.isSuccess
  }

  /** Load to BigQuery
    *
    * @param config
    */
  override def bqload(workflow: IngestionWorkflow, config: BigQueryLoadCliConfig)(implicit
    settings: Settings
  ): Boolean = {
    logger.info(s"Launch bq: ${config}")
    workflow.bqload(config.asBigqueryLoadConfig())
    true

  }

  /** Load to JDBC
    *
    * @param config
    */
  override def jdbcload(workflow: IngestionWorkflow, config: JdbcConnectionLoadConfig)(implicit
    settings: Settings
  ): Boolean = {
    logger.info(s"Launch JDBC: ${config}")
    workflow.jdbcload(config)
    true
  }
}
