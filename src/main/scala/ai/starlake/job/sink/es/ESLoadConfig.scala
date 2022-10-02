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

package ai.starlake.job.sink.es

import ai.starlake.config.Settings
import ai.starlake.schema.model.RowLevelSecurity
import ai.starlake.utils.CliConfig
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import scopt.OParser

import java.util.regex.Pattern

case class ESLoadConfig(
  timestamp: Option[String] = None,
  id: Option[String] = None,
  mapping: Option[Path] = None,
  domain: String = "",
  schema: String = "",
  format: String = "",
  dataset: Option[Either[Path, DataFrame]] = None,
  options: Map[String, String] = Map.empty,
  rls: Option[List[RowLevelSecurity]] = None
) {

  def getDataset()(implicit settings: Settings): Either[Path, DataFrame] = {
    dataset match {
      case None =>
        Left(
          new Path(s"${settings.comet.datasets}/${settings.comet.area.accepted}/$domain/$schema")
        )
      case Some(pathOrDF) =>
        pathOrDF
    }
  }

  private val timestampColPattern = Pattern.compile("\\{(.*)\\|(.*)\\}")
  def getTimestampCol(): Option[String] = {
    timestamp.flatMap { ts =>
      val matcher = timestampColPattern.matcher(ts)
      if (matcher.matches()) {
        Some(matcher.group(1))
      } else {
        None
      }
    }
  }
}

object ESLoadConfig extends CliConfig[ESLoadConfig] {
  val command = "esload"
  val parser: OParser[Unit, ESLoadConfig] = {
    val builder = OParser.builder[ESLoadConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("timestamp")
        .action((x, c) => c.copy(timestamp = Some(x)))
        .optional()
        .text("Elasticsearch index timestamp suffix as in {@timestamp|yyyy.MM.dd}"),
      opt[String]("id")
        .action((x, c) => c.copy(id = Some(x)))
        .optional()
        .text("Elasticsearch Document Id"),
      opt[String]("mapping")
        .action((x, c) => c.copy(mapping = Some(new Path(x))))
        .optional()
        .text("Path to Elasticsearch Mapping File"),
      opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .required()
        .text("Domain Name"),
      opt[String]("schema")
        .action((x, c) => c.copy(schema = x))
        .required()
        .text("Schema Name"),
      opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .required()
        .text("Dataset input file : parquet, json or json-array"),
      opt[String]("dataset")
        .action((x, c) => c.copy(dataset = Some(Left(new Path(x)))))
        .optional()
        .text("Input dataset path"),
      opt[Map[String, String]]("conf")
        .action((x, c) => c.copy(options = x))
        .optional()
        .valueName("es.batch.size.entries=1000, es.batch.size.bytes=1mb...")
        .text(
          """esSpark configuration options. See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html""".stripMargin
        )
    )
  }

  def parse(args: Seq[String]): Option[ESLoadConfig] =
    OParser.parse(parser, args, ESLoadConfig())
}
