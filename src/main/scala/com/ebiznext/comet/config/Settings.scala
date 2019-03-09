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

package com.ebiznext.comet.config

import com.ebiznext.comet.schema.handlers.{AirflowLauncher, LaunchHandler, SimpleLauncher}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import configs.syntax._

object Settings extends StrictLogging {

  /**
    *
    * @param endpoint : Airflow REST API endpoint, aka. http://127.0.0.1:8080/api/experimental
    */
  final case class Airflow(endpoint: String)

  /**
    * datasets in the data pipeline go through several stages and
    * are stored on disk at each of these stages.
    * This setting allow to customize the folder names of each of these stages.
    *
    * @param pending    : Name of the pending area
    * @param unresolved : Named of the unresolved area
    * @param archive    : Name of the archive area
    * @param ingesting  : Name of the ingesting area
    * @param accepted   : Name of the accepted area
    * @param rejected   : Name of the rejected area
    * @param business   : Name of the business area
    */
  final case class Area(
                         pending: String,
                         unresolved: String,
                         archive: String,
                         ingesting: String,
                         accepted: String,
                         rejected: String,
                         business: String
                       )

  /**
    *
    * @param discreteMaxCardinality : Max number of unique values allowed in cardinality compute
    *
    */
  final case class Metrics(path: String, discreteMaxCardinality: Int)

  /**
    *
    * @param datasets    : Absolute path, datasets root folder beneath which each area is defined.
    * @param metadata    : Absolute path, location where all types / domains and auto jobs are defined
    * @param metrics     : Absolute path, location where all computed metrics are stored
    * @param archive     : Should we backup the ingested datasets ? true by default
    * @param writeFormat : Choose between parquet, orc ... Default is parquet
    * @param launcher    : Cron Job Manager: simple (useful for testing) or airflow ? simple by default
    * @param analyze     : Should we create basics Hive statistics on the generated dataset ? true by default
    * @param hive        : Should we create a Hive Table ? true by default
    * @param area        : see Area above
    * @param airflow     : Airflow end point. Should be defined even if simple launccher is used instead of airflow.
    */
  final case class Comet(
                          datasets: String,
                          metadata: String,
                          metrics: Metrics,
                          archive: Boolean,
                          writeFormat: String,
                          launcher: String,
                          analyze: Boolean,
                          hive: Boolean,
                          area: Area,
                          airflow: Airflow
                        ) {

    def getLauncher(): LaunchHandler = launcher match {
      case "simple" => new SimpleLauncher
      case "airflow" => new AirflowLauncher
    }
  }

  val config: Config = ConfigFactory.load()

  val comet: Comet = config.extract[Comet].valueOrThrow { error =>
    error.messages.foreach(err => logger.error(err))
    throw new Exception("Failed to load config")
  }
  logger.info(s"Using Config $comet")

}
