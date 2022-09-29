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
package ai.starlake.schema.generator

import ai.starlake.utils.CliConfig
import scopt.OParser

case class Yml2DDLConfig(
  datawarehouse: String = "",
  outputPath: Option[String] = None,
  connection: Option[String] = None,
  domain: Option[String] = None,
  schemas: Option[Seq[String]] = None,
  catalog: Option[String] = None,
  apply: Boolean = false
)

object Yml2DDLConfig extends CliConfig[Yml2DDLConfig] {
  val command = "yml2ddl"
  val parser: OParser[Unit, Yml2DDLConfig] = {
    val builder = OParser.builder[Yml2DDLConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("datawarehouse")
        .action((x, c) => c.copy(datawarehouse = x))
        .required()
        .text("target datawarehouse name (ddl mapping key in types.yml"),
      opt[String]("connection")
        .action((x, c) => c.copy(connection = Some(x)))
        .optional()
        .text("JDBC connection name with at least read write on database schema"),
      opt[String]("output")
        .action((x, c) => c.copy(outputPath = Some(x)))
        .optional()
        .text("Where to output the generated files. ./$datawarehouse/ by default"),
      opt[String]("catalog")
        .action((x, c) => c.copy(catalog = Some(x)))
        .optional()
        .text("Database Catalog if any"),
      opt[String]("domain")
        .action((x, c) => c.copy(domain = Some(x)))
        .optional()
        .text("Domain to create DDL for. All by default")
        .children(
          opt[Seq[String]]("schemas")
            .action((x, c) => c.copy(schemas = Some(x)))
            .optional()
            .text("List of schemas to generate DDL for. All by default")
        ),
      opt[Unit]("apply")
        .action((x, c) => c.copy(apply = true))
        .optional()
        .text("Does the file contain a header (For CSV files only)")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class InferDDLConfig.
    */
  def parse(args: Seq[String]): Option[Yml2DDLConfig] =
    OParser.parse(parser, args, Yml2DDLConfig())
}
