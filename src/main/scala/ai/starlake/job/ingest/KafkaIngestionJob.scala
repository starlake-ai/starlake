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

package ai.starlake.job.ingest

import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{Domain, Mode, Schema, Type}
import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.kafka.KafkaClient
import ai.starlake.utils.{JobResult, Utils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._

import scala.util.{Failure, Try}

/** Main class to ingest JSON messages from Kafka
  *
  * @param domain
  *   : Output Dataset Domain
  * @param schema
  *   : Topic Name
  * @param types
  *   : List of globally defined types
  * @param path
  *   : Unused
  * @param storageHandler
  *   : Storage Handler
  */
class KafkaIngestionJob(
  domain: Domain,
  schema: Schema,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  options: Map[String, String],
  mode: Mode
)(implicit settings: Settings)
    extends JsonIngestionJob(domain, schema, types, path, storageHandler, schemaHandler, options) {

  var offsets: List[(Int, Long)] = Nil

  private val topicConfig: Settings.KafkaTopicConfig = settings.comet.kafka.topics(schema.name)

  /** Load dataset using spark csv reader and all metadata. Does not infer schema. columns not
    * defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return
    *   Spark DataFrame where each row holds a single string
    */
  override protected def loadJsonData(): Dataset[String] = {
    val dfIn = metadata.mode match {
      case None | Some(Mode.FILE) =>
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          val (dfIn, offsets) =
            kafkaClient.consumeTopicBatch(schema.name, session, topicConfig)
          this.offsets = offsets
          dfIn
        }
      case Some(Mode.STREAM) =>
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          KafkaClient.consumeTopicStreaming(session, topicConfig)
        }
      case _ =>
        throw new Exception("Should never happen")
    }
    logger.whenDebugEnabled {
      logger.debug(dfIn.schemaString())
    }
    val rddIn = dfIn.rdd.map { row =>
      row.getAs[String]("value")
    }
    logger.whenDebugEnabled {
      logger.debug(dfIn.schemaString())
    }
    session.createDataset(rddIn)(Encoders.STRING)
  }

  override def run(): Try[JobResult] = {
    val res = super.run()
    mode match {
      case Mode.FILE =>
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          kafkaClient.topicSaveOffsets(
            schema.name,
            topicConfig.allAccessOptions(),
            offsets
          )
          res
        }
      case Mode.STREAM =>
        res
      case _ =>
        Failure(throw new Exception("Should never happen"))
    }
  }
}
