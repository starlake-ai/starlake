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

package ai.starlake.job.sink.es

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Schema
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}
import org.apache.http.client.methods.{HttpDelete, HttpPut}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import scala.util.{Failure, Success, Try}

class ESLoadJob(
  cliConfig: ESLoadConfig,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends SparkJob {

  val path = cliConfig.getDataset()
  val format = cliConfig.format
  val dataset = cliConfig.dataset
  val domain = schemaHandler.getDomain(cliConfig.domain)
  val schema = domain.flatMap(_.tables.find(_.name == cliConfig.schema))

  override def name: String = s"Index $path"

  def getIndexName(): String =
    (domain, schema) match {
      case (Some(domain), Some(schema)) =>
        s"${domain.getFinalName().toLowerCase}.${schema.getFinalName().toLowerCase}"
      case _ =>
        // Handle datasets without YAML schema
        // We handle only index name like idx-{...}
        s"${cliConfig.domain.toLowerCase}.${cliConfig.schema.toLowerCase}"
    }

  def getResource(): String = {
    cliConfig.timestamp.map { ts =>
      s"${this.getIndexName()}-$ts"
    } getOrElse {
      s"${this.getIndexName()}"
    }
  }

  /** Just to force any spark job to implement its entry point within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    logger.info(s"Indexing resource ${getResource()} with $cliConfig")
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
              session.read.format("parquet").load(path.toString)

            case "delta" =>
              session.read.format("delta").load(path.toString)
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
      (domain, schema) match {
        case (Some(domain), Some(schema)) =>
          schema.esMapping(domain.esMapping(schema), domain.name, schemaHandler)
        case _ =>
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
    val esOptions = settings.comet.elasticsearch.options
    val host: String = esOptions.getOrElse("es.nodes", "localhost")
    val port = esOptions.getOrElse("es.port", "9200").toInt
    val ssl = esOptions.getOrElse("es.net.ssl", "false").toBoolean
    val protocol = if (ssl) "https" else "http"
    val username = esOptions.get("net.http.auth.user")
    val password = esOptions.get("net.http.auth.password")
    val client = HttpClients.createDefault
    import java.nio.charset.StandardCharsets
    import java.util.Base64

    val basicHeader = for {
      u <- username
      p <- password
    } yield {
      "Basic " + Base64.getEncoder.encodeToString("$u:$p".getBytes(StandardCharsets.UTF_8))

    }
    val templateURL = s"$protocol://$host:$port/_template/${getIndexName()}"
    val delRequest = new HttpDelete(templateURL)
    delRequest.setHeader("Content-Type", "application/json")
    basicHeader.foreach { basicHeader =>
      delRequest.setHeader("Authorization", basicHeader)
    }
    client.execute(delRequest)

    val putRequest = new HttpPut(templateURL)
    putRequest.setEntity(new StringEntity(content, ContentType.APPLICATION_JSON))
    basicHeader.foreach { basicHeader =>
      delRequest.setHeader("Authorization", basicHeader)
    }
    val responsePut = client.execute(putRequest)

    val ok = 200 to 299 contains responsePut.getStatusLine().getStatusCode()
    if (ok) {
      val esresource = Some(("es.resource.write", s"${getResource()}"))
      val esId = cliConfig.id.map("es.mapping.id" -> _)
      val esCliConf = cliConfig.options ++ List(esresource, esId).flatten.toMap
      val allConf = esOptions.toList ++ esCliConf.toList
      logger.whenDebugEnabled {
        logger.debug(s"sending ${df.count()} documents to Elasticsearch using $allConf")
      }
      val writer = df.write
        .options(allConf.toMap)
        .format("org.elasticsearch.spark.sql")
        .mode(SaveMode.Overwrite)
      writer.save(getResource())
      Success(SparkJobResult(None))
    } else {
      Failure(throw new Exception("Failed to create template"))
    }
  }
}
