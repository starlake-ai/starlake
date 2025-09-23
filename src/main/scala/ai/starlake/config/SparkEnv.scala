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

  private var _session: Option[SparkSession] = None

  def isSessionStarted() = _session.isDefined

  def closeSession() = {
    if (isSessionStarted()) {
      _session.get.close()
      _session = None
    }
  }

  /** Creates a Spark Session with the spark.* keys defined the application conf file.
    */
  def session: SparkSession = {
    if (!isSessionStarted()) {
      val sysProps = System.getProperties()
      if (!SparkSessionBuilder.isSparkConnectActive) {
        val localCatalog = jobConf.getOption("spark.localCatalog").getOrElse("none")
        // sys.env.getOrElse("SL_SPARK_LOCAL_CATALOG", "none")

        // We need to avoid in-memory catalog implementation otherwise delta will fail to work
        // through subsequent spark sessions since the metastore is not present anywhere.

        localCatalog match {
          case "hive" =>
            // Handled by configuration
            logger.info("Using Hive as local catalog")

            sysProps.setProperty("derby.system.home", datasetsArea)
            config.set("spark.sql.warehouse.dir", datasetsArea)
            config.set("spark.sql.catalogImplementation", "hive")

          case "none" =>
            // Default Spark catalog implementation
            logger.info("Using default Spark catalog implementation")
          case "delta" if !Utils.isDeltaAvailable() && !Utils.isRunningInDatabricks() =>
            logger.warn(
              "Delta catalog requested but Delta package not found. Using default Spark catalog implementation"
            )
          case "delta" if Utils.isRunningInDatabricks() =>
            logger.warn(
              "Delta catalog requested but running on Databricks. Using default Spark catalog implementation"
            )
          case "delta" =>
            logger.info("Using Delta as local catalog")

            sysProps.setProperty("derby.system.home", datasetsArea + "/delta")
            config.set("spark.sql.warehouse.dir", datasetsArea + "/delta")
            config.set("spark.sql.catalogImplementation", "hive")

            config.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            if (config.get("spark.sql.catalog.spark_catalog", "").isEmpty) {
              config.set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
              )
            }

          case "iceberg" if !Utils.isIcebergAvailable() =>
            logger.warn(
              "Iceberg catalog requested but Iceberg package not found. Using default Spark catalog implementation"
            )
          case "iceberg" =>
            // Handled by configuration
            logger.info("Using Iceberg as local catalog")

            sysProps.setProperty("derby.system.home", datasetsArea)
            config.set("spark.sql.warehouse.dir", datasetsArea)
            config.set("spark.sql.catalogImplementation", "hive")

            config.set(
              "spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            if (config.get("spark.sql.catalog.spark_catalog", "").isEmpty)
              config.set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog"
              )
            if (config.get("spark.sql.catalog.spark_catalog.type", "").isEmpty)
              config.set("spark.sql.catalog.spark_catalog.type", "hadoop")

            if (config.get("spark.sql.catalog.local", "").isEmpty)
              config.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            if (config.get("spark.sql.catalog.local.type", "").isEmpty)
              config.set("spark.sql.catalog.local.type", "hadoop")

            if (config.get("spark.sql.catalog.spark_catalog.warehouse", "").isEmpty)
              config.set("spark.sql.catalog.spark_catalog.warehouse", datasetsArea + "/iceberg")
            if (config.get("spark.sql.catalog.local.warehouse", "").isEmpty)
              config.set("spark.sql.catalog.local.warehouse", datasetsArea + "/iceberg")

            if (config.get("spark.sql.defaultCatalog", "").isEmpty)
              config.set("spark.sql.defaultCatalog", "local")

            // Enable Iceberg SQL syntax
            config.set("spark.sql.ansi.enabled", "true")

          case other if other != "none" =>
            logger.warn(s"Unknown local catalog $other. Using default Spark catalog implementation")
          case _ =>
            logger.info("Using default Spark catalog implementation")
        }
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

      _session = Some(SparkSessionBuilder.build(config))
    }
    _session.get
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
