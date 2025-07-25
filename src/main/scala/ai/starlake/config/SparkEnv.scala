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
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Any Spark Job will inherit from this class. All properties defined in application conf file and
  * prefixed by the "spark" key will be loaded into the Spark Job
  *
  * @param name
  *   : Cusom spark application name prefix. The current datetime is appended this the Spark Job
  *   name
  */
class SparkEnv private (
  name: String,
  jobConf: SparkConf,
  datasetsArea: String,
  confTransformer: SparkConf => SparkConf = identity
) extends LazyLogging {

  /** Load spark.* properties from the loaded application conf file
    */
  val config: SparkConf = confTransformer(jobConf)

  /** Creates a Spark Session with the spark.* keys defined the application conf file.
    */
  lazy val session: SparkSession = {
    val sysProps = System.getProperties()

    if (
      sys.env.getOrElse("SL_SPARK_NO_CATALOG", "false").toBoolean &&
      config.getOption("spark.sql.catalogImplementation").isEmpty
    ) {
      // We need to avoid in-memory catalog implementation otherwise delta will fail to work
      // through subsequent spark sessions since the metastore is not present anywhere.
      sysProps.setProperty("derby.system.home", datasetsArea)
      config.set("spark.sql.warehouse.dir", datasetsArea)
    }

    if (Utils.isIcebergAvailable()) {
      // Handled by configuration
    } else if (!Utils.isRunningInDatabricks() && Utils.isDeltaAvailable()) {
      config.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      if (config.get("spark.sql.catalog.spark_catalog", "").isEmpty)
        config.set(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    }

    // spark.sql.catalogImplementation = in-memory incompatible with delta on multiple spark sessions
    /*
    val catalogs = settings.sparkConfig.getString("sql.catalogKeys").split(",").toList
      if (
        settings.appConfig.isHiveCompatible()
        && catalogs.exists(config.contains)
        && !Utils.isRunningInDatabricks() /* no need to enable hive support on databricks */
      )
        builder.enableHiveSupport().getOrCreate()
      else
        builder.getOrCreate()
     */

    // hive support on databricks, spark local, hive metastore

    val session = SparkSessionBuilder.build(config)
    session
  }
}

object SparkEnv {
  var sparkEnv: SparkEnv = _
  def get(
    name: String,
    confTransformer: SparkConf => SparkConf = identity,
    settings: Settings
  ): SparkEnv = {
    if (sparkEnv == null) {
      sparkEnv = new SparkEnv(name, settings.jobConf, settings.appConfig.datasets, confTransformer)
    }
    sparkEnv
  }
}
