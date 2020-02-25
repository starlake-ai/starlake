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

package com.ebiznext.comet.job.atlas

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.atlas.AtlasModel
import com.typesafe.scalalogging.StrictLogging

class AtlasJob(
  cliConfig: AtlasConfig,
  storageHandler: StorageHandler
)(implicit /* TODO: make me explicit */ settings: Settings)
    extends StrictLogging {

  def run(): Unit = {
    logger.info(s"")
    val uris = cliConfig.uris.map(_.toArray).getOrElse(Array(Settings.comet.atlas.uri))
    val userPassword = (cliConfig.user, cliConfig.password) match {
      case (Some(user), Some(pwd)) => Array(user, pwd)
      case _                       => Array(Settings.comet.atlas.user, Settings.comet.atlas.password)
    }
    new AtlasModel(uris, userPassword).run(cliConfig, storageHandler)
  }
}
