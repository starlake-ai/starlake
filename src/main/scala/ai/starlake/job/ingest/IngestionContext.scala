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

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{DomainInfo, Mode, SchemaInfo, Type}
import org.apache.hadoop.fs.Path

case class IngestionContext(
  domain: DomainInfo,
  schema: SchemaInfo,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  options: Map[String, String],
  accessToken: Option[String],
  test: Boolean,
  scheduledDate: Option[String]
)(implicit val settings: Settings)

/** Factory for creating ingestion jobs. All formats are now handled by DuckDbNativeLoader which is
  * invoked from IngestionJob.run(). The factory creates a single IngestionJob that dispatches to
  * the appropriate native loader based on the sink connection.
  */
object IngestionJobFactory {

  def createJob(context: IngestionContext, mode: Option[Mode] = None): IngestionJob = {
    implicit val settings: Settings = context.settings
    new DummyIngestionJob(
      context.domain,
      context.schema,
      context.types,
      context.path,
      context.storageHandler,
      context.schemaHandler,
      context.options,
      context.accessToken,
      context.test,
      context.scheduledDate
    )
  }
}
