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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

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
          new Path(
            s"${settings.appConfig.datasets}/${settings.appConfig.area.accepted}/$domain/$schema"
          )
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
