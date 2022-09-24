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

package ai.starlake.schema.model

import ai.starlake.config.{DatasetArea, Settings, StorageArea}
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path

/** Task executed in the context of a job. Each task is executed in its own session.
  *
  * @param sql
  *   Main SQL request to exexute (do not forget to prefix table names with the database name to
  *   avoid conflicts)
  * @param domain
  *   Output domain in output Area (Will be the Database name in Hive or Dataset in BigQuery)
  * @param dataset
  *   Dataset Name in output Area (Will be the Table name in Hive & BigQuery)
  * @param write
  *   Append to or overwrite existing data
  * @param area
  *   Target Area where domain / dataset will be stored.
  * @param partition
  *   List of columns used for partitioning the outtput.
  * @param presql
  *   List of SQL requests to executed before the main SQL request is run
  * @param postsql
  *   List of SQL requests to executed after the main SQL request is run
  * @param sink
  *   Where to sink the data
  * @param rls
  *   Row level security policy to apply too the output data.
  */
case class AutoTaskDesc(
  name: Option[String],
  sql: Option[String],
  domain: String,
  table: String,
  write: WriteMode,
  partition: Option[List[String]] = None,
  presql: Option[List[String]] = None,
  postsql: Option[List[String]] = None,
  area: Option[StorageArea] = None,
  sink: Option[Sink] = None,
  rls: Option[List[RowLevelSecurity]] = None,
  assertions: Option[Map[String, String]] = None,
  engine: Option[Engine] = None,
  acl: Option[List[AccessControlEntry]] = None
) {

  @JsonIgnore
  def getPartitions(): List[String] = partition.getOrElse(Nil)

  def getSql(): String = sql.getOrElse("")

  /** Return a Path only if a storage area s defined
    * @param defaultArea
    * @param settings
    * @return
    */
  def getTargetPath(defaultArea: StorageArea)(implicit settings: Settings): Path = {
    new Path(DatasetArea.path(domain, area.getOrElse(defaultArea).value), table)
  }

  def getHiveDB(defaultArea: StorageArea)(implicit settings: Settings): String = {
    StorageArea.area(domain, area.getOrElse(defaultArea))
  }
}

/** A job is a set of transform tasks executed using the specified engine.
  *
  * @param name:
  *   Job logical name
  * @param tasks
  *   List of transform tasks to execute
  * @param area
  *   Area where the data is located. When using the BigQuery engine, teh area corresponds to the
  *   dataset name we will be working on in this job. When using the Spark engine, this is folder
  *   where the data should be store. Default value is "business"
  * @param format
  *   output file format when using Spark engine. Ingored for BigQuery. Default value is "parquet"
  * @param coalesce
  *   When outputting files, should we coalesce it to a single file. Useful when CSV is the output
  *   format.
  * @param udf
  *   : Register UDFs written in this JVM class when using Spark engine Register UDFs stored at this
  *   location when using BigQuery engine
  * @param views
  *   : Create temporary views using where the key is the view name and the map the SQL request
  *   corresponding to this view using the SQL engine supported syntax.
  * @param engine
  *   : SPARK or BQ. Default value is SPARK.
  */
case class AutoJobDesc(
  name: String,
  tasks: List[AutoTaskDesc],
  area: Option[StorageArea] = None,
  format: Option[String],
  coalesce: Option[Boolean],
  udf: Option[String] = None,
  views: Option[Map[String, String]] = None,
  engine: Option[Engine] = None,
  require: Option[List[String]] = None,
  taskRefs: Option[List[String]] = None
) {

  def getArea(): StorageArea = area.getOrElse(StorageArea.business)

  def getEngine(): Engine = engine.getOrElse(Engine.SPARK)
}
