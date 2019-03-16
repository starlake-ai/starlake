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

package com.ebiznext.comet.job.index

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.utils.SparkJob
import com.softwaremill.sttp._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class IndexJob(
                cliConfig: IndexConfig,
                storageHandler: StorageHandler
              ) extends SparkJob {


  val esresource = Some(("es.resource.write", s"${cliConfig.resource}"))
  val esId = cliConfig.id.map("es.mapping.id" -> _)
  val esCliConf = cliConfig.conf ++ List(esresource, esId).flatten.toMap
  val path = cliConfig.getDataset()
  val format = cliConfig.format

  override def name: String = s"Index $path"

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): SparkSession = {
    val df = format match {
      case "json" =>
        session.read
          .option("multiline", true)
          .json(path.toString)

      case "json-array" =>
        val jsonRDD =
          session.sparkContext.wholeTextFiles(path.toString).map(x => x._2)
        session.read.json(jsonRDD)

      case "parquet" =>
        session.read.parquet(path.toString)
    }

    val content = cliConfig.mapping.map(storageHandler.read).getOrElse {
      val dynamicTemplate = for {
        domain <- Settings.schemaHandler.getDomain(cliConfig.domain)
        schema <- domain.schemas.find(_.name == cliConfig.schema)
      } yield schema.mapping(domain.mapping(schema))

      dynamicTemplate.getOrElse(throw new Exception("Should never happen"))
    }

    import scala.collection.JavaConverters._
    val esOptions = Settings.comet.elasticsearch.options.asScala.toMap
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

    val request = authSttp.getOrElse(sttp).
      body(content).
      put(uri"$protocol://$host:$port/_template/${cliConfig.domain}_${cliConfig.schema}").
      contentType("application/json")

    val response = request.send()
    val ok = (200 to 299) contains response.code

    val allConf = esOptions.toList ++ esCliConf.toList
    allConf.foldLeft(df.write)((w, kv) => w.option(kv._1, kv._2)).
      format("org.elasticsearch.spark.sql").
      mode("overwrite").
      save(cliConfig.getResource())

    session
  }
}
