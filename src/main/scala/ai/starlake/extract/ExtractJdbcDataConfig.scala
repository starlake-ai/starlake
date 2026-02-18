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

import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.job.ReportFormatConfig
import ai.starlake.schema.model.{JDBCSchema, PrimitiveType, TableAttribute}
import org.apache.hadoop.fs.Path

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

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
  fullExport: Option[Boolean] = None,
  ifExtractedBefore: Option[Long] = None,
  ignoreExtractionFailure: Boolean = false,
  cleanOnExtract: Boolean = false,
  includeSchemas: Seq[String] = Seq.empty,
  excludeSchemas: Seq[String] = Seq.empty,
  includeTables: Seq[String] = Seq.empty,
  excludeTables: Seq[String] = Seq.empty,
  reportFormat: Option[String] = None
) extends ReportFormatConfig

/** Class used during data extraction process
  */
case class ExtractJdbcDataConfig(
  jdbcSchema: JDBCSchema,
  baseOutputDir: Path,
  limit: Int,
  numPartitions: Int,
  parallelism: Option[Int],
  cliFullExport: Option[Boolean],
  extractionPredicate: Option[Long => Boolean],
  ignoreExtractionFailure: Boolean,
  cleanOnExtract: Boolean,
  includeTables: Seq[String],
  excludeTables: Seq[String],
  outputFormat: FileFormat,
  data: ConnectionInfo,
  audit: ConnectionInfo
)

/** Information related to how the table should be extracted. We've got partitionned table and
  * unpartitionned table.
  */
case class TableExtractDataConfig(
  domain: String,
  table: String,
  sql: Option[String],
  columnsProjection: List[TableAttribute],
  fullExport: Boolean,
  fetchSize: Option[Int],
  tableOutputDir: Path,
  filterOpt: Option[String],
  partitionConfig: Option[PartitionConfig]
) {

  val extractionDateTime: String = {
    val formatter = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(ZoneId.systemDefault())
    formatter.format(Instant.now())
  }

  def columnsProjectionQuery(connectionSettings: ConnectionInfo): String = columnsProjection
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

  lazy val partitionColumn: String = partitionConfig
    .map(_.partitionColumn)
    .getOrElse(throw new RuntimeException("Partition column not defined"))

  lazy val partitionColumnType: PrimitiveType = partitionConfig
    .map(_.partitionColumnType)
    .getOrElse(throw new RuntimeException("Partition column type not defined"))

  val hashFunc: Option[String] = partitionConfig.flatMap(_.hashFunc)

  lazy val nbPartitions: Int = partitionConfig
    .map(_.nbPartitions)
    .getOrElse(throw new RuntimeException("Partition column type not defined"))
}

case class PartitionConfig(
  partitionColumn: String,
  partitionColumnType: PrimitiveType,
  hashFunc: Option[String],
  nbPartitions: Int
)

case class ExtractTableAttributes(
  tableRemarks: Option[String],
  columNames: List[TableAttribute],
  primaryKeys: List[String],
  filterOpt: Option[String]
)
