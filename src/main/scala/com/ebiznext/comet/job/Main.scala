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

package com.ebiznext.comet.job

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.atlas.AtlasConfig
import com.ebiznext.comet.job.bqload.BigQueryLoadConfig
import com.ebiznext.comet.job.index.IndexConfig
import com.ebiznext.comet.job.infer.InferSchemaConfig
import com.ebiznext.comet.job.metrics.MetricsConfig
import com.ebiznext.comet.utils.FileLock
import com.ebiznext.comet.workflow.IngestionWorkflow
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

/**
  * The root of all things.
  *  - importing from landing
  *  - submitting requests to the cron manager
  *  - ingesting the datasets
  *  - running an auto job
  * All these things ared laaunched from here.
  * See printUsage below to understand the CLI syntax.
  *
  */
object Main extends StrictLogging {
  // uses Jackson YAML to parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)

  private def printUsage() = {
    // scalastyle:off println
    println(
      """
        |Usage :
        |comet job jobname
        |comet watch [+/-DOMAIN1,DOMAIN2,...]
        |comet import
        |comet ingest datasetDomain datasetSchema datasetPath
        |comet index --domain domain --schema schema --timestamp {@timestamp|yyyy.MM.dd} --id type-id --mapping mapping --format parquet|json|json-array --dataset datasetPath --conf key=value,key=value,...
        |comet bqload --source_file file --output_table table --source_format parquet --create_disposition CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
        |comet infer-schema --domain domainName --schema schemaName --input datasetpath --output outputPath --with-header headerBoolean
        |comet metrics --domain domain --schema schema
        |      """.stripMargin
    )
    // scalastyle:on println
  }

  /**
    * @param args depends on the action required
    *             to run a job:
    *   - call "comet job jobname" where jobname is the name of the job
    *             as defined in one of the definition files present in the metadata/jobs folder.
    *             to import files from a local file system
    *   - call "comet import", this will move files in the landing area to the pending area
    *             to watch for files wiating to be processed
    *   - call"comet watch [{+|–}domain1,domain2,domain3]" with a optional domain list separated by a ','.
    *             When called without any domain, will watch for all domain folders in the landing area
    *             When called with a '+' sign, will look only for this domain folders in the landing area
    *             When called with a '-' sign, will look for all domain folder in the landing area except the ones in the command lines.
    *   - call "comet ingest domain schema hdfs://datasets/domain/pending/file.dsv"
    *             to ingest a file defined by its schema in the specified domain
    *   -call "comet infer-schema --domain domainName --schema schemaName --input datasetpath --output outputPath --with-header boolean
    *   - call "comet metrics --domain domain-name --schema schema-name "
    *             to compute all metrics on specific schema in a specific domain
    */
  def main(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    import settings.{launcherService, schemaHandler, storageHandler}
    DatasetArea.init(storageHandler)

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val workflow =
      new IngestionWorkflow(storageHandler, schemaHandler, launcherService)

    if (args.length == 0) printUsage()

    val arglist = args.toList
    logger.info(s"Running Comet $arglist")
    arglist.head match {
      case "job" if arglist.length == 2 => workflow.autoJob(arglist(1))
      case "import"                     => workflow.loadLanding()
      case "watch" =>
        if (arglist.length == 2) {
          val param = arglist(1)
          if (param.startsWith("-"))
            workflow.loadPending(Nil, param.substring(1).split(',').toList)
          else if (param.startsWith("+"))
            workflow.loadPending(param.substring(1).split(',').toList, Nil)
          else
            workflow.loadPending(param.split(',').toList, Nil)
        } else
          workflow.loadPending()
      case "ingest" if arglist.length == 4 =>
        val domain = arglist(1)
        val schema = arglist(2)
        val paths = arglist(3)
        val lockPath = new Path(settings.comet.lock.path, s"${domain}_${schema}.lock")
        val locker = new FileLock(lockPath, storageHandler)
        val waitTimeMillis = settings.comet.lock.ingestionTimeout
        val ingestResult =
          if (locker.tryLock(waitTimeMillis)) {
            Try {
              workflow.ingest(domain, schema, paths.split(',').map(new Path(_)).toList)
            }
          } else {
            Failure(
              new Exception(
                s"Failed to obtain lock on file $lockPath waited (millis) $waitTimeMillis"
              )
            )
          }
        locker.release()
        ingestResult match {
          case Failure(e) =>
            throw e
          case Success(_) =>
          // do nothing
        }

      case "index" =>
        IndexConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.index(config)
          case _ =>
            printUsage()
        }

      case "atlas" =>
        AtlasConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.atlas(config)
          case _ =>
            printUsage()
        }

      case "bqload" =>
        BigQueryLoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.bqload(config)
          case _ =>
            printUsage()
        }

      case "infer-schema" => {
        InferSchemaConfig.parse(args.drop(1)) match {
          case Some(config) => workflow.infer(config)
          case _            => printUsage()
        }
      }
      case "metrics" =>
        MetricsConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.metric(config)
          case _ =>
            printUsage()
        }

      case _ => printUsage()
    }
  }
}
