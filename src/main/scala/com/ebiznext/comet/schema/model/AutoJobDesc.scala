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

import com.ebiznext.comet.config.HiveArea
import com.fasterxml.jackson.annotation.JsonIgnore

/**
  * Task executed in teh context of a job
  *
  * @param sql     SQL request to exexute (do not forget to prefix table names with the database name
  * @param domain  Output domain in Business Area (Will be the Database name in Hive)
  * @param dataset Dataset Name in Business Area (Will be the Table name in Hive)
  * @param write   Append to or overwrite existing dataset
  */
case class AutoTask(
                     sql: String,
                     domain: String,
                     dataset: String,
                     write: WriteMode,
                     partition: Option[List[String]]= None,
                     presql: Option[List[String]] = None,
                     postsql: Option[List[String]] = None,
                     area: Option[HiveArea] = None,
                     index: Option[Boolean] = None,
                     mapping: Option[EsMapping] = None

                   ) {

  @JsonIgnore
  def getPartitions() = partition.getOrElse(Nil)

  @JsonIgnore
  def isIndexed() = index.getOrElse(false)


}

/**
  *
  * @param name  Job logical name
  * @param tasks List of business tasks to execute
  */
case class AutoJobDesc(name: String, tasks: List[AutoTask], area: Option[HiveArea] = None) {
  def getArea() = area.getOrElse(HiveArea.business)
}
