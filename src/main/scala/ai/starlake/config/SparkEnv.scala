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

package ai.starlake.config

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

/** Any Spark Job will inherit from this class. All properties defined in application conf file and
  * prefixed by the "spark" key will be loaded into the Spark Job
  *
  * @param name
  *   : Cusom spark application name prefix. The current datetime is appended this the Spark Job
  *   name
  */
class SparkEnv(name: String, confTransformer: SparkConf => SparkConf = identity)(implicit
  settings: Settings
) extends StrictLogging {

  private def initSchedulingConfig(): File = {
    import settings.comet.scheduling._

    val schedulingConfig =
      if (allocations.isEmpty)
        s"""<?xml version="1.0"?>
                     |<allocations>
                     |  <pool name="starlake">
                     |    <schedulingMode>$mode</schedulingMode>
                     |    <weight>$weight}</weight>
                     |    <minShare>$minShare</minShare>
                     |  </pool>
                     |</allocations>""".stripMargin
      else
        allocations

    val tempFile = File.temp / "starlake-spark-scheduling.xml"
    tempFile.overwrite(schedulingConfig)
  }

  /** Load spark.* properties from the loaded application conf file
    */
  val config: SparkConf = {
    val schedulingConfig = initSchedulingConfig()
    val now = LocalDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS"))
    val appName = s"$name-$now"

    // When using local Spark with remote BigQuery (useful for testing)
    val initialConf =
      sys.env.get("TEMPORARY_GCS_BUCKET") match {
        case Some(value) => new SparkConf().set("temporaryGcsBucket", value)
        case None        => new SparkConf()
      }

    val thisConf = settings.sparkConfig
      .entrySet()
      .asScala
      .to[Vector]
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .foldLeft(initialConf) { case (conf, (key, value)) => conf.set("spark." + key, value) }
      .setAppName(appName)
      .set("spark.app.id", appName)
      .set("spark.scheduler.allocation.file", schedulingConfig.uri.toString)

    val withExtraConf = confTransformer(thisConf)

    logger.whenDebugEnabled {
      logger.debug(withExtraConf.toDebugString)
    }
    withExtraConf
  }

  /** Creates a Spark Session with the spark.* keys defined the application conf file.
    */
  lazy val session: SparkSession = {
    val session =
      if (settings.comet.hive)
        SparkSession.builder.config(config).enableHiveSupport().getOrCreate()
      else {
        SparkSession.builder.config(config).getOrCreate()
      }
    logger.info("Spark Version -> " + session.version)
    logger.info(session.conf.getAll.mkString("\n"))
    session
  }

}
