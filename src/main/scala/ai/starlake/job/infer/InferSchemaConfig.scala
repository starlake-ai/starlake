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
package ai.starlake.job.infer

import ai.starlake.utils.CliConfig
import scopt.OParser

case class InferSchemaConfig(
  domainName: String = "",
  schemaName: String = "",
  inputPath: String = "",
  outputPath: String = "",
  header: Option[Boolean] = Some(false)
)

object InferSchemaConfig extends CliConfig[InferSchemaConfig] {
  val command = "infer-schema"
  val parser: OParser[Unit, InferSchemaConfig] = {
    val builder = OParser.builder[InferSchemaConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("domain")
        .action((x, c) => c.copy(domainName = x))
        .required()
        .text("Domain Name"),
      opt[String]("schema")
        .action((x, c) => c.copy(schemaName = x))
        .required()
        .text("Schema Name"),
      opt[String]("input")
        .action((x, c) => c.copy(inputPath = x))
        .required()
        .text("Dataset Input Path"),
      opt[String]("output")
        .action((x, c) => c.copy(outputPath = x))
        .required()
        .text("Domain YAML Output Path"),
      opt[Option[Boolean]]("with-header")
        .action((x, c) => c.copy(header = x))
        .optional()
        .text("Does the file contain a header (For CSV files only)")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class InferSchemaConfig.
    */
  def parse(args: Seq[String]): Option[InferSchemaConfig] =
    OParser.parse(parser, args, InferSchemaConfig())
}
