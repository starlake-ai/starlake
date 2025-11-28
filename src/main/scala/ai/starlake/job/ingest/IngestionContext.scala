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
import ai.starlake.schema.model.{DomainInfo, Format, Mode, SchemaInfo, Type}
import org.apache.hadoop.fs.Path

/** Encapsulates all common parameters needed to create an ingestion job.
  *
  * ==Design Pattern==
  * This class implements the '''Parameter Object''' pattern to reduce the number of parameters
  * passed to IngestionJob constructors. Instead of passing 10 individual parameters to each
  * format-specific ingestion job, callers create a single IngestionContext instance.
  *
  * ==Usage==
  * {{{
  * // Create context with all parameters
  * val context = IngestionContext(
  *   domain = myDomain,
  *   schema = mySchema,
  *   types = globalTypes,
  *   path = List(inputPath),
  *   storageHandler = storageHandler,
  *   schemaHandler = schemaHandler,
  *   options = Map("key" -> "value"),
  *   accessToken = None,
  *   test = false,
  *   scheduledDate = None
  * )
  *
  * // Create appropriate job using factory
  * val job = IngestionJobFactory.createJob(context)
  * }}}
  *
  * ==Supported Formats==
  * The factory creates different job types based on the format in metadata:
  *   - PARQUET -> ParquetIngestionJob
  *   - DSV (CSV) -> DsvIngestionJob
  *   - JSON/JSON_FLAT -> JsonIngestionJob
  *   - XML -> XmlIngestionJob
  *   - POSITION (fixed-width) -> PositionIngestionJob
  *   - KAFKA/KAFKASTREAM -> KafkaIngestionJob
  *   - GENERIC -> GenericIngestionJob
  *
  * @param domain
  *   Input Dataset Domain containing domain-level metadata and configuration
  * @param schema
  *   Input Dataset Schema defining the structure, attributes, and validation rules
  * @param types
  *   List of globally defined types for data type mapping and validation
  * @param path
  *   Input dataset path(s) - can be multiple files for batch processing
  * @param storageHandler
  *   Storage Handler for file operations (HDFS, S3, GCS, local filesystem)
  * @param schemaHandler
  *   Schema Handler for schema operations and domain/table lookups
  * @param options
  *   Parameters to pass as input (e.g., date ranges, partition values)
  * @param accessToken
  *   Optional OAuth access token for authenticated storage access
  * @param test
  *   Whether this is a test run (enables DuckDB transpilation)
  * @param scheduledDate
  *   Optional scheduled date for the job (used in audit logging)
  */
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

/** Factory for creating ingestion jobs based on the data format.
  *
  * ==Design Pattern==
  * This object implements the '''Factory Pattern''' to centralize ingestion job creation.
  * Previously, the IngestionWorkflow contained a large match statement with 10+ cases, each
  * duplicating the same 10 constructor parameters. This factory eliminates that duplication by
  * extracting the format-based selection logic.
  *
  * ==Benefits==
  *   - Single point of change when adding new formats or modifying parameters
  *   - Reduced code duplication (from ~125 lines to ~15 lines in callers)
  *   - Easier testing of format selection logic
  *   - Clear separation of concerns
  */
object IngestionJobFactory {

  /** Creates the appropriate ingestion job based on the format specified in metadata.
    *
    * The format is determined by merging the schema's metadata with the domain's metadata, then
    * calling `resolveFormat()`. This allows format to be specified at either level.
    *
    * ==Format Resolution==
    * The format can be explicitly set in YAML or inferred from file extension:
    *   - `.csv`, `.tsv` -> DSV
    *   - `.json` -> JSON
    *   - `.parquet` -> PARQUET
    *   - `.xml` -> XML
    *
    * @param context
    *   The ingestion context containing all necessary parameters
    * @param mode
    *   Optional mode for Kafka ingestion (FILE for batch, STREAM for streaming)
    * @return
    *   The appropriate IngestionJob instance for the detected format
    * @throws IllegalArgumentException
    *   if the format is not supported
    */
  def createJob(context: IngestionContext, mode: Option[Mode] = None): IngestionJob = {
    // Extract settings from context for implicit parameter
    implicit val settings: Settings = context.settings
    // Merge schema metadata with domain metadata to get effective configuration
    val metadata = context.schema.mergedMetadata(context.domain.metadata)

    metadata.resolveFormat() match {
      case Format.PARQUET =>
        new ParquetIngestionJob(
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

      case Format.GENERIC =>
        new GenericIngestionJob(
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

      case Format.DSV =>
        new DsvIngestionJob(
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

      case Format.JSON_FLAT | Format.JSON =>
        new JsonIngestionJob(
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

      case Format.XML =>
        new XmlIngestionJob(
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

      case Format.TEXT_XML =>
        new XmlSimplePrivacyJob(
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

      case Format.POSITION =>
        new PositionIngestionJob(
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

      // Kafka batch mode - reads from Kafka topics as if they were files
      case Format.KAFKA =>
        new KafkaIngestionJob(
          context.domain,
          context.schema,
          context.types,
          context.path,
          context.storageHandler,
          context.schemaHandler,
          context.options,
          mode.getOrElse(Mode.FILE), // Default to FILE mode for batch processing
          context.accessToken,
          context.test,
          context.scheduledDate
        )

      // Kafka streaming mode - continuous processing of Kafka messages
      case Format.KAFKASTREAM =>
        new KafkaIngestionJob(
          context.domain,
          context.schema,
          context.types,
          context.path,
          context.storageHandler,
          context.schemaHandler,
          context.options,
          mode.getOrElse(Mode.STREAM), // Default to STREAM mode for continuous processing
          context.accessToken,
          context.test,
          context.scheduledDate
        )

      case other =>
        // Fail fast with clear error message for unsupported formats
        throw new IllegalArgumentException(s"Unsupported format: $other")
    }
  }
}
