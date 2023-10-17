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

import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

  /** Load spark.* properties from the loaded application conf file
    */
  val config: SparkConf = confTransformer(settings.jobConf)

  /** Creates a Spark Session with the spark.* keys defined the application conf file.
    */
  lazy val session: SparkSession = {
    if (!Utils.isRunningInDatabricks() && Utils.isDeltaAvailable()) {
      config.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      config.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
    }
    val session =
      if (settings.appConfig.isHiveCompatible())
        SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
      else {
        SparkSession.builder().config(config).getOrCreate()
      }

    logger.info("Spark Version -> " + session.version)
    logger.info(session.conf.getAll.mkString("\n"))
    session
  }
}
