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
import better.files.File
import scopt.OParser

/** @param files
  *   List of Excel files
  */
case class Xls2YmlConfig(
  files: Seq[String] = Nil,
  iamPolicyTagsFile: Option[String] = None,
  outputPath: Option[String] = None,
  policyFile: Option[String] = None,
  job: Boolean = false
)

object Xls2YmlConfig extends CliConfig[Xls2YmlConfig] {
  val command = "xls2yml"

  val parser: OParser[Unit, Xls2YmlConfig] = {
    val builder = OParser.builder[Xls2YmlConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[Seq[String]]("files")
        .action { (x, c) =>
          val allFiles = x.flatMap { f =>
            val file = File(f)
            if (file.isDirectory()) {
              file.collectChildren(_.name.endsWith(".xlsx")).toList
            } else if (file.exists) {
              List(file)
            } else {
              throw new IllegalArgumentException(s"File $file does not exist")
            }
          }

          c.copy(files = allFiles.map(_.pathAsString))
        }
        .required()
        .text("List of Excel files describing domains & schemas or jobs"),
      opt[String]("iamPolicyTagsFile")
        .action((x, c) => c.copy(iamPolicyTagsFile = Some(x)))
        .optional()
        .text("If true generate IAM PolicyTags YML"),
      opt[String]("outputPath")
        .action((x, c) => c.copy(outputPath = Some(x)))
        .optional()
        .text(
          """Path for saving the resulting YAML file(s). Starlake domains path is used by default.""".stripMargin
        ),
      opt[String]("policyFile")
        .action((x, c) => c.copy(policyFile = Some(x)))
        .optional()
        .text(
          """Optional File for centralising ACL & RLS definition.""".stripMargin
        ),
      opt[Boolean]("job")
        .action((x, c) => c.copy(job = x))
        .optional()
        .text("If true generate YML for a Job.")
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
