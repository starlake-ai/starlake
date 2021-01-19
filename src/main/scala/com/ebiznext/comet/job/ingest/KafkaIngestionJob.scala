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

package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.kafka.KafkaTopicUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._

/** Main class to ingest delimiter separated values file
  *
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
  */
class KafkaIngestionJob(
  domain: Domain,
  schema: Schema,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  options: Map[String, String]
)(implicit settings: Settings)
    extends JsonIngestionJob(domain, schema, types, path, storageHandler, schemaHandler, options) {

  /** Load dataset using spark csv reader and all metadata. Does not infer schema.
    * columns not defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return Spark DataFrame where each row holds a single string
    */
  override protected def loadJsonData(): Dataset[String] = {
    val dfIn = new KafkaTopicUtils(settings.comet.kafka.serverOptions)
      .consumeTopic(schema.name, session, settings.comet.kafka.topics(schema.name))

    val rddIn = dfIn.rdd.map { row =>
      row.getString(1)
    }

    logger.debug(dfIn.schema.treeString)
    import org.apache.spark.sql._
    session.read.json(session.createDataset(rddIn)(Encoders.STRING)).toJSON
  }
}
