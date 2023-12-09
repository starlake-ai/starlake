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

/** @param files
  *   List of Excel files
  * @param encryption
  *   Should pre & post encryption YAML be generated ?
  * @param delimiter
  *   : Delimiter to use on generated CSV file after pre-encryption.
  * @param privacy
  *   What privacy policies are to be applied at the pre-encrypt step ? All by default.
  */
case class DagGenerateConfig(
  outputDir: Option[String] = None,
  clean: Boolean = false
)

object DagGenerateConfig extends CliConfig[DagGenerateConfig] {
  val command = "dag-generate"

  val parser: OParser[Unit, DagGenerateConfig] = {
    val builder = OParser.builder[DagGenerateConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text(
          """Path for saving the resulting DAG file(s).""".stripMargin
        ),
      opt[Unit]("clean")
        .action((x, c) => c.copy(clean = true))
        .optional()
        .text(
          """Clean Resulting DAg file output first ?""".stripMargin
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[DagGenerateConfig] =
    OParser.parse(parser, args, DagGenerateConfig(), setup)
}
