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
case class Xls2YmlConfig(
  files: Seq[String] = Nil,
  encryption: Boolean = false,
  delimiter: Option[String] = None,
  privacy: Seq[String] = Nil,
  outputPath: Option[String] = None
)

object Xls2YmlConfig extends CliConfig[Xls2YmlConfig] {

  val parser: OParser[Unit, Xls2YmlConfig] = {
    val builder = OParser.builder[Xls2YmlConfig]
    import builder._
    OParser.sequence(
      programName("starlake xls2yml"),
      head("starlake", "xls2yml", "[options]"),
      note(""),
      opt[Seq[String]]("files")
        .action((x, c) => c.copy(files = x))
        .required()
        .text("List of Excel files describing Domains & Schemas"),
      opt[Boolean]("encryption")
        .action((x, c) => c.copy(encryption = x))
        .optional()
        .text("If true generate pre and post encryption YML"),
      opt[String]("delimiter")
        .action((x, c) => c.copy(delimiter = Some(x)))
        .optional()
        .text("CSV delimiter to use in post-encrypt YML."),
      opt[Seq[String]]("privacy")
        .action((x, c) => c.copy(privacy = x map (_.toUpperCase)))
        .optional()
        .text(
          """What privacy policies should be applied in the pre-encryption phase ? All privacy policies are applied by default.""".stripMargin
        ),
      opt[Option[String]]("outputPath")
        .action((x, c) => c.copy(outputPath = x))
        .optional()
        .text(
          """Path for saving the resulting YAML file(s). Comet domains path is used by default.""".stripMargin
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Xls2YmlConfig] =
    OParser.parse(parser, args, Xls2YmlConfig())
}
