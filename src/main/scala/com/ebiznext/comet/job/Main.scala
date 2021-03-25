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

import buildinfo.BuildInfo
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.extractor.ScriptGen
import com.ebiznext.comet.job.atlas.AtlasConfig
import com.ebiznext.comet.job.convert.{Parquet2CSV, Parquet2CSVConfig}
import com.ebiznext.comet.job.index.bqload.BigQueryLoadConfig
import com.ebiznext.comet.job.index.connectionload.ConnectionLoadConfig
import com.ebiznext.comet.job.index.esload.ESLoadConfig
import com.ebiznext.comet.job.index.kafkaload.{KafkaJob, KafkaJobConfig}
import com.ebiznext.comet.job.infer.InferSchemaConfig
import com.ebiznext.comet.job.ingest.LoadConfig
import com.ebiznext.comet.job.metrics.MetricsConfig
import com.ebiznext.comet.schema.generator.{Xls2Yml, Xls2YmlConfig, Yml2XlsConfig, Yml2XlsWriter}
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.ebiznext.comet.utils.{CometObjectMapper, FileLock}
import com.ebiznext.comet.workflow.{ImportConfig, IngestionWorkflow, TransformConfig, WatchConfig}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

/** The root of all things.
  *  - importing from landing
  *  - submitting requests to the cron manager
  *  - ingesting the datasets
  *  - running an auto job
  * All these things ared laaunched from here.
  * See printUsage below to understand the CLI syntax.
  */
object Main extends StrictLogging {
  // uses Jackson YAML to parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper = new CometObjectMapper(new YAMLFactory())

  private def printUsage() = {
    // scalastyle:off println
    println(
      s"""
        |Usage : One of
        |${LoadConfig.usage()}
        |${ImportConfig.usage()}
        |${TransformConfig.usage()}
        |${WatchConfig.usage()}
        |${ESLoadConfig.usage()}
        |${BigQueryLoadConfig.usage()}
        |${InferSchemaConfig.usage()}
        |${MetricsConfig.usage()}
        |${Parquet2CSVConfig.usage()}
        |${Xls2YmlConfig.usage()}
        |${Yml2XlsConfig.usage()}
        |${KafkaJobConfig.usage()}
        |""".stripMargin
    )
    // scalastyle:on println
  }

  /** @param args depends on the action required
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
    logger.info(s"Comet Version ${BuildInfo.version}")
    import settings.{launcherService, metadataStorageHandler, storageHandler}
    DatasetArea.initMetadata(metadataStorageHandler)
    val schemaHandler = new SchemaHandler(metadataStorageHandler)

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val workflow =
      new IngestionWorkflow(storageHandler, schemaHandler, launcherService)

    if (args.length == 0) printUsage()

    val arglist = args.toList
    logger.info(s"Running Comet $arglist")
    val result = arglist.head match {
      case "job" | "transform" =>
        TransformConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.autoJob(config)
          case _ =>
            println(TransformConfig.usage())
            false
        }
      case "import" =>
        workflow.loadLanding()
        true
      case "watch" =>
        WatchConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.loadPending(config)
          case _ =>
            println(WatchConfig.usage())
            false
        }
      case "ingest" | "load" =>
        LoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            val lockPath =
              new Path(settings.comet.lock.path, s"${config.domain}_${config.schema}.lock")
            val locker = new FileLock(lockPath, storageHandler)
            val waitTimeMillis = settings.comet.lock.ingestionTimeout

            locker.doExclusively(waitTimeMillis) {
              workflow.ingest(config)
            }

          case _ =>
            println(LoadConfig.usage())
            false
        }
      case "index" | "esload" =>
        ESLoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.esLoad(config).isSuccess
          case _ =>
            println(ESLoadConfig.usage())
            false
        }

      case "atlas" =>
        AtlasConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.atlas(config)
          case _ =>
            println(AtlasConfig.usage())
            false
        }

      case "kafkaload" =>
        KafkaJobConfig.parse(args.drop(1)) match {
          case Some(config) =>
            new KafkaJob(config).run().isSuccess

          case _ =>
            println(KafkaJobConfig.usage())
            false
        }

      case "bqload" =>
        BigQueryLoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.bqload(config).isSuccess
          case _ =>
            println(BigQueryLoadConfig.usage())
            false
        }

      case "cnxload" =>
        ConnectionLoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.jdbcload(config).isSuccess
          case _ =>
            println(ConnectionLoadConfig.usage())
            false
        }

      case "infer-schema" => {
        InferSchemaConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.infer(config).isSuccess
          case _ =>
            println(InferSchemaConfig.usage())
            false
        }
      }
      case "metrics" =>
        MetricsConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.metric(config).isSuccess
          case _ =>
            println(MetricsConfig.usage())
            false
        }

      case "parquet2csv" =>
        Parquet2CSVConfig.parse(args.drop(1)) match {
          case Some(config) =>
            new Parquet2CSV(config, storageHandler).run().isSuccess
          case _ =>
            println(Parquet2CSVConfig.usage())
            false
        }

      case "xls2yml" =>
        Xls2Yml.run(args.drop(1))

      case "yml2xls" =>
        new Yml2XlsWriter(schemaHandler).run(args.drop(1))
        true

      case "extract" =>
        ScriptGen.run(args.drop(1))
      case _ =>
        printUsage()
        false
    }
    System.exit(if (result) 0 else 1)
  }
}
