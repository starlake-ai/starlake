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

package com.ebiznext.comet.schema.model

import com.ebiznext.comet.config.{DatasetArea, Settings, StorageArea}
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path

/**
  * Task executed in teh context of a job
  *
  * @param sql     SQL request to exexute (do not forget to prefix table names with the database name
  * @param domain  Output domain in Business Area (Will be the Database name in Hive or Dataset in BigQuery)
  * @param dataset Dataset Name in Business Area (Will be the Table name in Hive & BigQuery)
  * @param write   Append to or overwrite existing dataset
  * @param area   Target Area where domain / dataset will be stored
  */
case class AutoTaskDesc(
  sql: String,
  domain: String,
  dataset: String,
  write: WriteMode,
  partition: Option[List[String]] = None,
  presql: Option[List[String]] = None,
  postsql: Option[List[String]] = None,
  area: Option[StorageArea] = None,
  sink: Option[Sink] = None,
  rls: Option[RowLevelSecurity] = None
) {

  @JsonIgnore
  def getPartitions(): List[String] = partition.getOrElse(Nil)

  /**
    * Return a Path only if a storage area s defined
    * @param defaultArea
    * @param settings
    * @return
    */
  def getTargetPath(defaultArea: Option[StorageArea])(implicit settings: Settings): Option[Path] = {
    area.orElse(defaultArea).map { targetArea =>
      new Path(DatasetArea.path(domain, targetArea.value), dataset)
    }
  }

  def getHiveDB(defaultArea: Option[StorageArea]): Option[String] = {
    area.orElse(defaultArea).map { targetArea =>
      StorageArea.area(domain, targetArea)
    }
  }
}

/**
  * @param name  Job logical name
  * @param tasks List of business tasks to execute
  */
case class AutoJobDesc(
  name: String,
  tasks: List[AutoTaskDesc],
  area: Option[StorageArea] = None,
  format: Option[String],
  coalesce: Option[Boolean],
  udf: Option[String] = None,
  views: Option[Map[String, String]] = None,
  engine: Option[Engine] = None
) {

  def getArea(): StorageArea = area.getOrElse(StorageArea.business)

  def getEngine(): Engine = engine.getOrElse(Engine.SPARK)
}
