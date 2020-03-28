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

package com.ebiznext.comet.job.ingest


import buildinfo.BuildInfo
import com.ebiznext.comet.utils.CliConfig
import org.apache.hadoop.fs.Path
import scopt.OParser

/**
  *
  * @param domain     domain name of the dataset
  * @param schema     schema name of the dataset
  * @param paths      Absolute path of the file to ingest (present in the ingesting area of the domain)
  */
case class IngestConfig(
  domain: String = "",
  schema: String = "",
  paths: List[Path] = Nil
)

object IngestConfig extends CliConfig[IngestConfig] {

  val parser: OParser[Unit, IngestConfig] = {
    val builder = OParser.builder[IngestConfig]
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", BuildInfo.version),
      arg[String]("domain")
        .required()
        .action((x, c) => c.copy(domain = x))
        .text("Domain name"),
      arg[String]("schema")
        .required()
        .action((x, c) => c.copy(schema = x))
        .text("Schema name"),
      arg[String]("paths")
        .required()
        .action((x, c) => c.copy(paths = x.split(',').map(new Path(_)).toList))
        .text("list of comma separated paths")
    )
  }

  def parse(args: Seq[String]): Option[IngestConfig] = {
    OParser.parse(parser, args, IngestConfig())
  }

  def main(args: Array[String]): Unit = {
    println(IngestConfig.usage())
  }
}
