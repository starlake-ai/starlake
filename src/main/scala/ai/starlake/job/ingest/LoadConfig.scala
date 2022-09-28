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

package ai.starlake.job.ingest

import ai.starlake.utils.CliConfig
import org.apache.hadoop.fs.Path
import scopt.OParser

/** @param domain
  *   domain name of the dataset
  * @param schema
  *   schema name of the dataset
  * @param paths
  *   Absolute path of the file to ingest (present in the ingesting area of the domain)
  */
case class LoadConfig(
  domain: String = "",
  schema: String = "",
  paths: List[Path] = Nil,
  options: Map[String, String] = Map.empty
)

object LoadConfig extends CliConfig[LoadConfig] {
  val command = "load"
  val parser: OParser[Unit, LoadConfig] = {
    val builder = OParser.builder[LoadConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      arg[String]("domain")
        .required()
        .action((x, c) => c.copy(domain = x))
        .text("Domain name"),
      arg[String]("schema")
        .required()
        .action((x, c) => c.copy(schema = x))
        .text("Schema name"),
      arg[String]("paths")
        .optional() // Some Ingestion Engine are not based on paths.$ eq. JdbcIngestionJob
        .action((x, c) => c.copy(paths = x.split(',').map(new Path(_)).toList))
        .text("list of comma separated paths"),
      arg[Map[String, String]]("options")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[LoadConfig] = {
    OParser.parse(parser, args, LoadConfig())
  }

  def main(args: Array[String]): Unit = {
    println(LoadConfig.usage())
  }
}
