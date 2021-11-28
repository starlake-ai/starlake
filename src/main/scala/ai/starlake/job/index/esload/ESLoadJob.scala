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

package ai.starlake.job.index.esload

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Schema
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}
import com.softwaremill.sttp._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ESLoadJob(
  cliConfig: ESLoadConfig,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends SparkJob {

  val esresource = Some(("es.resource.write", s"${cliConfig.getResource()}"))
  val esId = cliConfig.id.map("es.mapping.id" -> _)
  val esCliConf = cliConfig.options ++ List(esresource, esId).flatten.toMap
  val path = cliConfig.getDataset()
  val format = cliConfig.format
  val dataset = cliConfig.dataset

  override def name: String = s"Index $path"

  /** Just to force any spark job to implement its entry point within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    logger.info(s"Indexing resource ${cliConfig.getResource()} with $cliConfig")
    val inputDF =
      path match {
        case Left(path) =>
          format match {
            case "json" =>
              session.read
                .option("multiline", value = true)
                .json(path.toString)

            case "json-array" =>
              val jsonDS = session.read.textFile(path.toString)
              session.read.json(jsonDS)

            case "parquet" =>
              session.read.parquet(path.toString)
          }
        case Right(df) =>
          df
      }

    // Convert timestamp field to ISO8601 date time, so that ES Hadoop can handle it correctly.
    val df = cliConfig.getTimestampCol().map { tsCol =>
      inputDF
        .withColumn("comet_es_tmp", date_format(col(tsCol), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .drop(tsCol)
        .withColumnRenamed("comet_es_tmp", tsCol)
    } getOrElse inputDF

    val content = cliConfig.mapping.map(storageHandler.read).getOrElse {
      val dynamicTemplate = for {
        domain <- schemaHandler.getDomain(cliConfig.domain)
        schema <- domain.schemas.find(_.name == cliConfig.schema)
      } yield schema.esMapping(domain.esMapping(schema), domain.name, schemaHandler)

      dynamicTemplate.getOrElse {
        // Handle datasets without YAML schema
        // We handle only index name like idx-{...}
        Schema.mapping(
          cliConfig.domain,
          cliConfig.schema,
          StructField("ignore", df.schema),
          schemaHandler
        )
      }
    }
    logger.info(
      s"Registering template ${cliConfig.domain.toLowerCase}_${cliConfig.schema.toLowerCase} -> $content"
    )
    val esOptions = settings.comet.elasticsearch.options.asScala.toMap
    val host: String = esOptions.getOrElse("es.nodes", "localhost")
    val port = esOptions.getOrElse("es.port", "9200").toInt
    val ssl = esOptions.getOrElse("es.net.ssl", "false").toBoolean
    val protocol = if (ssl) "https" else "http"
    val username = esOptions.get("net.http.auth.user")
    val password = esOptions.get("net.http.auth.password")

    implicit val backend = HttpURLConnectionBackend()
    val authSttp = for {
      u <- username
      p <- password
    } yield {
      sttp.auth.basic(u, p)
    }

    val templateUri =
      uri"$protocol://$host:$port/_template/${cliConfig.getIndexName()}"
    val requestDel = authSttp
      .getOrElse(sttp)
      .delete(templateUri)
      .contentType("application/json")
    val _ = requestDel.send()

    val requestPut = authSttp
      .getOrElse(sttp)
      .body(content)
      .put(templateUri)
      .contentType("application/json")

    val responsePut = requestPut.send()
    val ok = (200 to 299) contains responsePut.code
    if (ok) {
      val allConf = esOptions.toList ++ esCliConf.toList
      logger.whenDebugEnabled {
        logger.debug(s"sending ${df.count()} documents to Elasticsearch using $allConf")
      }
      val writer = df.write
        .options(allConf.toMap)
        .format("org.elasticsearch.spark.sql")
        .mode(SaveMode.Overwrite)
      if (settings.comet.isElasticsearchSupported())
        writer.save(cliConfig.getResource())
      Success(SparkJobResult(None))
    } else {
      Failure(throw new Exception("Failed to create template"))
    }
  }
}
