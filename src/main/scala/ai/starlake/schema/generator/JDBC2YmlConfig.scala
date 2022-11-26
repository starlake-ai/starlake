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

case class JDBC2YmlConfig(
  mapping: String = "",
  outputDir: Option[String] = None,
  ymlTemplate: Option[String] = None,
  mode: String = "schema",
  limit: Int = 0,
  separator: String = ";"
)

object JDBC2YmlConfig extends CliConfig[JDBC2YmlConfig] {
  val command = "extract"
  val parser: OParser[Unit, JDBC2YmlConfig] = {
    val builder = OParser.builder[JDBC2YmlConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[Unit]("data")
        .text("Export table data")
        .action((x, c) => c.copy(mode = "data"))
        .optional()
        .children(
          opt[String]("mapping")
            .action((x, c) => c.copy(mapping = x))
            .required()
            .text("Database tables & connection info"),
          opt[Int]("limit")
            .action((x, c) => c.copy(limit = x))
            .optional()
            .text("Limit number of records"),
          opt[String]("separator")
            .action((x, c) => c.copy(separator = x))
            .optional()
            .text("Column separator"),
          opt[String]("output-dir")
            .action((x, c) => c.copy(outputDir = Some(x)))
            .required()
            .text("Where to output csv files")
        ),
      opt[Unit]("schema")
        .text("Export table schema")
        .action((x, c) => c.copy(mode = "schema"))
        .optional()
        .children(
          opt[String]("mapping")
            .action((x, c) => c.copy(mapping = x))
            .required()
            .text("Database tables & connection info"),
          opt[String]("output-dir")
            .action((x, c) => c.copy(outputDir = Some(x)))
            .optional()
            .text("Where to output YML files"),
          opt[String]("template")
            .action((x, c) => c.copy(ymlTemplate = Some(x)))
            .optional()
            .text("YML template to use YML metadata")
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class JDBC2YmlConfig.
    */
  def parse(args: Seq[String]): Option[JDBC2YmlConfig] =
    OParser.parse(parser, args, JDBC2YmlConfig())
}
