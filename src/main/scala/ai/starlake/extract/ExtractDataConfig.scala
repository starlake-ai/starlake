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
package ai.starlake.extract

import ai.starlake.config.Settings.Connection
import ai.starlake.schema.model.{Attribute, PrimitiveType}
import better.files.File

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

/** This class defined the configuration provided by the end user from CLI
  *
  * @param extractConfig
  *   the extract configuration file name in the extract folder used for data extraction.
  * @param outputDir
  *   the base output directory of fetched data
  * @param limit
  *   the maximum number of elements retrieved in a table
  * @param numPartitions
  *   the number of partitions to use when fetching a table in parallel. Can be overrided in table
  *   definition or jdbc schema
  * @param parallelism
  *   the number of tables to process in parallel
  * @param fullExport
  *   indicates wether we should export the table in full or not. By default, it is true
  * @param ifExtractedBefore
  *   datetime to compare with. If last succesful extraction datetime is more recent than given
  *   datetime, extraction is skipped.
  * @param ignoreExtractionFailure
  *   specify the behavior on any failures occurring during extraction.
  * @param cleanOnExtract
  *   clean all files related to the table if the table is extracted
  * @param includeSchemas
  *   schemas to include. Any schema not matching this list are excluded.
  * @param excludeSchemas
  *   schemas to exclude. Any schema not matching this list are included.
  * @param includeTables
  *   tables to include. Any table not matching this list are excluded.
  * @param excludeTables
  *   tables to exclude. Any table not matching this list are included.
  */
case class UserExtractDataConfig(
  extractConfig: String = "",
  outputDir: Option[String] = None,
  limit: Int = 0,
  numPartitions: Int = 1,
  parallelism: Option[Int] = None,
  fullExport: Boolean = true,
  ifExtractedBefore: Option[Long] = None,
  ignoreExtractionFailure: Boolean = false,
  cleanOnExtract: Boolean = false,
  includeSchemas: Seq[String] = Seq.empty,
  excludeSchemas: Seq[String] = Seq.empty,
  includeTables: Seq[String] = Seq.empty,
  excludeTables: Seq[String] = Seq.empty
)

/** Class used during data extraction process
  */
case class ExtractDataConfig(
  jdbcSchema: JDBCSchema,
  baseOutputDir: File,
  limit: Int,
  numPartitions: Int,
  parallelism: Option[Int],
  fullExport: Boolean,
  extractionPredicate: Option[Long => Boolean],
  ignoreExtractionFailure: Boolean,
  cleanOnExtract: Boolean,
  includeTables: Seq[String],
  excludeTables: Seq[String],
  outputFormat: FileFormat,
  data: Connection,
  audit: Connection
)

/** Information related to how the table should be extracted. We've got partitionned table and
  * unpartitionned table.
  */
sealed trait TableExtractDataConfig {
  def domain: String
  def table: String
  def columnsProjection: List[Attribute]
  def fullExport: Boolean
  def fetchSize: Option[Int]

  val extractionDateTime: String = {
    val formatter = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(ZoneId.systemDefault())
    formatter.format(Instant.now())
  }

  def columnsProjectionQuery(connectionSettings: Connection): String = columnsProjection
    .map(c =>
      c.name -> c.rename match {
        case (name, Some(newName)) =>
          connectionSettings.quoteIdentifier(name) + " as " + connectionSettings.quoteIdentifier(
            newName
          )
        case (name, _) => connectionSettings.quoteIdentifier(name)
      }
    )
    .mkString(",")
}
case class UnpartitionnedTableExtractDataConfig(
  domain: String,
  table: String,
  columnsProjection: List[Attribute],
  fullExport: Boolean,
  fetchSize: Option[Int]
) extends TableExtractDataConfig {}

case class PartitionnedTableExtractDataConfig(
  domain: String,
  table: String,
  columnsProjection: List[Attribute],
  fullExport: Boolean,
  fetchSize: Option[Int],
  partitionColumn: String,
  partitionColumnType: PrimitiveType,
  hashFunc: Option[String],
  nbPartitions: Int
) extends TableExtractDataConfig
