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

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.index.bqload.BigQueryLoadConfig
import com.ebiznext.comet.job.index.connectionload.ConnectionLoadConfig
import com.ebiznext.comet.job.index.esload.ESLoadConfig
import com.ebiznext.comet.job.ingest.LoadConfig
import com.ebiznext.comet.schema.model.{Domain, Schema}
import com.ebiznext.comet.utils.{AirflowJobResult, JobResult, Utils}
import com.ebiznext.comet.workflow.IngestionWorkflow
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import scala.util.Try

/** Interface required for any cron job launcher
  */
trait LaunchHandler {

  /** Submit to the cron manager a single file for ingestion
    *
    * @param domain : Domain to which belong this dataset
    * @param schema : Schema of the dataset
    * @param path   : absolute path where the source dataset  (JSON / CSV / ...) is located
    * @return success / failure
    */
  def ingest(
    workflow: IngestionWorkflow,
    domain: Domain,
    schema: Schema,
    path: Path,
    options: Map[String, String]
  )(implicit
    settings: Settings
  ): Option[Try[JobResult]] =
    ingest(workflow, domain, schema, path :: Nil, options)

  /** Submit to the cron manager multiple files for ingestion.
    * All the files should have the schema schema and belong to the same domain.
    *
    * @param domain : Domain to which belong this dataset
    * @param schema : Schema of the dataset
    * @param paths  : absolute paths where the source datasets  (JSON / CSV / ...) are located
    * @return success / failure
    */
  def ingest(
    workflow: IngestionWorkflow,
    domain: Domain,
    schema: Schema,
    paths: List[Path],
    options: Map[String, String]
  )(implicit settings: Settings): Option[Try[JobResult]]

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
  def bqload(workflow: IngestionWorkflow, config: BigQueryLoadConfig)(implicit
    settings: Settings
  ): Boolean

  /** Load to JDBC Database
    *
    * @param config
    */
  def jdbcload(workflow: IngestionWorkflow, config: ConnectionLoadConfig)(implicit
    settings: Settings
  ): Boolean
}

/** Simple Launcher will directly invoke the ingestion method wityhout using a cron manager.
  * This is userfull for testing purpose
  */
class SimpleLauncher extends LaunchHandler with StrictLogging {

  /** call directly the main assembly with the "ingest" parameter
    *
    * @param domain : Domain to which belong this dataset
    * @param schema : Schema of the dataset
    * @param paths  : absolute paths where the source datasets  (JSON / CSV / ...) are located
    * @return success / failure
    */
  override def ingest(
    workflow: IngestionWorkflow,
    domain: Domain,
    schema: Schema,
    paths: List[Path],
    options: Map[String, String]
  )(implicit settings: Settings): Option[Try[JobResult]] = {
    logger.info(s"Launch Ingestion: ${domain.name} ${schema.name} $paths ")
    workflow.ingest(LoadConfig(domain.name, schema.name, paths, options))
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
  override def bqload(workflow: IngestionWorkflow, config: BigQueryLoadConfig)(implicit
    settings: Settings
  ): Boolean = {
    logger.info(s"Launch bq: ${config}")
    workflow.bqload(config)
    true

  }

  /** Load to JDBC
    *
    * @param config
    */
  override def jdbcload(workflow: IngestionWorkflow, config: ConnectionLoadConfig)(implicit
    settings: Settings
  ): Boolean = {
    logger.info(s"Launch JDBC: ${config}")
    workflow.jdbcload(config)
    true

  }
}

/** Airflow Launcher will submit a request for ingestion to Airflow
  * using the REST API. The requested DAG must exist in Airflow first.
  */
class AirflowLauncher extends LaunchHandler with StrictLogging {

  protected def post(url: String, command: String): Try[AirflowJobResult] = {
    Try {
      val json = s"""{"conf":"{\\"command\\":\\"$command\\"}"}"""
      logger.info(s"JSON to post to Airflow: $json")
      val client = HttpClients.createDefault
      val requestEntity = new StringEntity(json, ContentType.APPLICATION_JSON);
      val httpPost = new HttpPost(url)
      httpPost.setEntity(requestEntity)
      logger.debug(
        "Posting to Airflow: " + httpPost.getURI.toString + "\n" + EntityUtils
          .toString(httpPost.getEntity, "UTF-8")
      )
      val response = client.execute(httpPost)
      val responseBody = EntityUtils.toString(response.getEntity, "UTF-8")
      logger.debug("Post result from Airflow: " + responseBody)
      client.close();
      AirflowJobResult(responseBody)
    }
  }

  /** Request the execution of the "comet-ingest" DAG in Airflow
    *
    * @param domain : Domain to which belong this dataset
    * @param schema : Schema of the dataset
    * @param paths  : absolute paths where the source datasets  (JSON / CSV / ...) are located
    * @return success if request accepted
    */
  override def ingest(
    workflow: IngestionWorkflow,
    domain: Domain,
    schema: Schema,
    paths: List[Path],
    options: Map[String, String]
  )(implicit settings: Settings): Option[Try[JobResult]] = {
    val endpoint = settings.comet.airflow.endpoint
    val ingest = settings.comet.airflow.ingest
    val url = s"$endpoint/dags/$ingest/dag_runs"
    val command =
      s"""ingest ${domain.name} ${schema.name} ${paths.mkString(",")}"""

    // We make sure two successive calls to the same dag id do not occur in the same second, otherwise Airflow will produce an error.
    Thread.sleep(1000)
    Some(post(url, command))
  }

  /** Index into elasticsearch
    *
    * @param config
    */
  override def esLoad(workflow: IngestionWorkflow, config: ESLoadConfig)(implicit
    settings: Settings
  ): Boolean = {
    val endpoint = settings.comet.airflow.endpoint
    val url = s"$endpoint/dags/comet_index/dag_runs"
    // comet index --domain domain --schema schema --resource index-name/type-name --id type-id --mapping mapping
    //    --format parquet|json|json-array --dataset datasetPath
    //    --conf key=value,key=value,...
    val resource = List(
      s"--timestamp ${config.timestamp}",
      s"--domain ${config.domain}",
      s"--schema ${config.schema}",
      s"--format ${config.format}",
      s"--dataset ${config.getDataset()}"
    ).mkString(" ")
    val id = config.id.map(id => s"--id $id")
    val mapping = config.mapping.map(path => s"--mapping ${path.toString}")
    val params = List(Some(resource), id, mapping).flatten.mkString(" ")
    val command = s"""index $params """
    post(url, command).isSuccess
  }

  /** Load to BigQuery
    *
    * @param config
    */
  override def bqload(workflow: IngestionWorkflow, config: BigQueryLoadConfig)(implicit
    settings: Settings
  ): Boolean = {
    val endpoint = settings.comet.airflow.endpoint
    val url = s"$endpoint/dags/comet_bqload/dag_runs"
    val params = List(
      s"--source_file ${config.source}",
      s"--output_dataset ${config.outputDataset}",
      s"--output_table ${config.outputTable}",
      s"--source_format ${config.sourceFormat}",
      s"--create_disposition ${config.createDisposition}",
      s"--write_disposition ${config.writeDisposition}",
      config.outputPartition.map(partition => s"--output_partition $partition").getOrElse("")
    ).mkString(" ")
    val command = s"""bqload $params """
    post(url, command).isSuccess
  }

  /** Load to JDBC sink
    *
    * @param config
    */
  override def jdbcload(workflow: IngestionWorkflow, config: ConnectionLoadConfig)(implicit
    settings: Settings
  ): Boolean = {
    val endpoint = settings.comet.airflow.endpoint
    val url = s"$endpoint/dags/comet_jdbcload/dag_runs"
    val params = List(
      s"--source_file ${config.sourceFile}",
      s"--options partitions=1000,user=sa,password=sa,batch_size=1,driver=org.postgresqlDriver,url=jdbc:postgresql:...",
      s"--format=jdbc",
      s"--create_disposition ${config.createDisposition}",
      s"--write_disposition ${config.writeDisposition}"
    ).mkString(" ")
    val command = s"""jdbcload $params """
    post(url, command).isSuccess
  }

}
