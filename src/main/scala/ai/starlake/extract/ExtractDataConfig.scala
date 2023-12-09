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

import ai.starlake.utils.CliConfig
import org.joda.time.DateTime
import scopt.OParser

case class ExtractDataConfig(
  extractConfig: String = "",
  outputDir: Option[String] = None,
  limit: Int = 0,
  numPartitions: Int = 1,
  parallelism: Option[Int] = None,
  fullExport: Boolean = true,
  ifExtractedBefore: Option[Long] = None,
  cleanOnExtract: Boolean = false,
  includeSchemas: Seq[String] = Seq.empty,
  excludeSchemas: Seq[String] = Seq.empty,
  includeTables: Seq[String] = Seq.empty,
  excludeTables: Seq[String] = Seq.empty
)

object ExtractDataConfig extends CliConfig[ExtractDataConfig] {
  val command = "extract-data"
  val parser: OParser[Unit, ExtractDataConfig] = {
    val builder = OParser.builder[ExtractDataConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |Extract data from any database defined in mapping file.
          |
          |Extraction is done in parallel by default and use all the available processors. It can be changed using `parallelism` CLI config.
          |Extraction of a table can be divided in smaller chunk and fetched in parallel by defining partitionColumn and its numPartitions.
          |
          |Examples
          |========
          |
          |Objective: Extract data
          |
          |  starlake.sh extract-data --config my-config --outputDir $PWD/output
          |
          |Objective: Plan to fetch all data but with different scheduling (once a day for all and twice a day for some) with failure recovery like behavior.
          |  starlake.sh extract-data --config my-config --outputDir $PWD/output --includeSchemas aSchema
          |         --includeTables table1RefreshedTwiceADay,table2RefreshedTwiceADay --ifExtractedBefore "2023-04-21 12:00:00"
          |         --clean
          |
          |""".stripMargin
      ),
      opt[String]("config")
        .action((x, c) => c.copy(extractConfig = x))
        .required()
        .text("Database tables & connection info"),
      opt[Int]("limit")
        .action((x, c) => c.copy(limit = x))
        .optional()
        .text("Limit number of records"),
      opt[Int]("numPartitions")
        .action((x, c) => c.copy(numPartitions = x))
        .optional()
        .text("parallelism level regarding partitionned tables"),
      opt[Int]("parallelism")
        .action((x, c) => c.copy(parallelism = Some(x)))
        .optional()
        .text(
          s"parallelism level of the extraction process. By default equals to the available cores: ${Runtime.getRuntime().availableProcessors()}"
        ),
      opt[Unit]("clean")
        .action((x, c) => c.copy(cleanOnExtract = true))
        .optional()
        .text("Clean all files of table only when it is extracted."),
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .required()
        .text("Where to output csv files"),
      opt[Unit]("incremental")
        .action((x, c) => c.copy(fullExport = false))
        .optional()
        .text("Export only new data since last extraction."),
      opt[String]("ifExtractedBefore")
        .action((x, c) => c.copy(ifExtractedBefore = Some(DateTime.parse(x).getMillis)))
        .optional()
        .text(
          "DateTime to compare with the last beginning extraction dateTime. If it is before that date, extraction is done else skipped."
        ),
      opt[Seq[String]]("includeSchemas")
        .action((x, c) => c.copy(includeSchemas = x.map(_.trim)))
        .valueName("schema1,schema2")
        .optional()
        .text("Domains to include during extraction."),
      opt[Seq[String]]("excludeSchemas")
        .valueName("schema1,schema2...")
        .optional()
        .action((x, c) => c.copy(excludeSchemas = x.map(_.trim)))
        .text(
          "Domains to exclude during extraction. if `include-domains` is defined, this config is ignored."
        ),
      opt[Seq[String]]("includeTables")
        .valueName("table1,table2,table3...")
        .optional()
        .action((x, c) => c.copy(includeTables = x.map(_.trim)))
        .text("Schemas to include during extraction."),
      opt[Seq[String]]("excludeTables")
        .valueName("table1,table2,table3...")
        .optional()
        .action((x, c) => c.copy(excludeTables = x.map(_.trim)))
        .text(
          "Schemas to exclude during extraction. if `include-schemas` is defined, this config is ignored."
        ),
      checkConfig { c =>
        val domainChecks = (c.excludeSchemas, c.includeSchemas) match {
          case (Nil, Nil) | (_, Nil) | (Nil, _) => Nil
          case _ => List("You can't specify includeSchemas and excludeSchemas at the same time.")
        }
        val schemaChecks = (c.excludeTables, c.includeTables) match {
          case (Nil, Nil) | (_, Nil) | (Nil, _) => Nil
          case _ => List("You can't specify includeTables and excludeTables at the same time.")
        }
        val allErrors = domainChecks ++ schemaChecks
        if (allErrors.isEmpty) {
          success
        } else {
          failure(allErrors.mkString("\n"))
        }
      }
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class JDBC2YmlConfig.
    */
  def parse(args: Seq[String]): Option[ExtractDataConfig] = {
    OParser.parse(parser, args, ExtractDataConfig(), setup)
  }

}
