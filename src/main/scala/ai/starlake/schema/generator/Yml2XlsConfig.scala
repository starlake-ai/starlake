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

/** @param domains
  *   : YML Input to convert
  * @param xls
  *   Excel file to produce
  */
case class Yml2XlsConfig(domains: Seq[String] = Nil, xlsDirectory: String = "")

object Yml2XlsConfig extends CliConfig[Yml2XlsConfig] {
  override val command: String = "yml2xls"

  val parser: OParser[Unit, Yml2XlsConfig] = {
    val builder = OParser.builder[Yml2XlsConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", "$command", "[options]"),
      note(""),
      opt[Seq[String]]("domain")
        .action((x, c) => c.copy(domains = x))
        .optional()
        .text("domains to convert to XLS"),
      opt[String]("xls")
        .action((x, c) => c.copy(xlsDirectory = x))
        .required()
        .text("directory where XLS files are generated")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Yml2XlsConfig] =
    OParser.parse(parser, args, Yml2XlsConfig())

}
