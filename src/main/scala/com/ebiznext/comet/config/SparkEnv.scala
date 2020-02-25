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

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Any Spark Job will inherit from this class.
  * All properties defined in application conf file and prefixed by the "spark" key will be loaded into the Spark Job
  *
  * @param name : Cusom spark application name prefix. The current datetime is appended this the Spark Job name
  */
class SparkEnv(name: String)(implicit /* TODO: make this explicit */ settings: Settings)
    extends StrictLogging {

  /**
    * Load spark.* properties rom the application conf file
    */
  val config: SparkConf = {
    val now = LocalDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS"))
    val appName = s"$name-$now"
    val thisConf = new SparkConf()

    thisConf.setAppName(appName)

    import scala.collection.JavaConverters._
    ConfigFactory
      .load()
      .getConfig("spark")
      .entrySet()
      .asScala
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .foreach {
        case (key, value) =>
          thisConf.set("spark." + key, value)
      }
    thisConf.set("spark.app.id", appName)
    logger.whenDebugEnabled {
      thisConf.getAll.foreach(x => logger.debug(x._1 + "=" + x._2))
    }
    thisConf
  }

  /**
    * Creates a Spark Session with the spark.* keys defined the applciation conf file.
    */
  lazy val session: SparkSession = {
    val session =
      if (Settings.comet.hive)
        SparkSession.builder.config(config).enableHiveSupport().getOrCreate()
      else
        SparkSession.builder.config(config).getOrCreate()
    logger.info("Spark Version -> " + session.version)
    logger.info(session.conf.getAll.mkString("\n"))
    session
  }

}
