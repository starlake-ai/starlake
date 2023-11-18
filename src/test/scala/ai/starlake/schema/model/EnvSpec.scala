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

package ai.starlake.schema.model

import ai.starlake.TestHelper
import ai.starlake.utils.{Utils, YamlSerializer}
import org.scalatest.BeforeAndAfterAll

import java.io.InputStream

class EnvSpec extends TestHelper with BeforeAndAfterAll {
  var env: Env = _

  override def beforeAll(): Unit = {
    new WithSettings() {
      val stream: InputStream =
        getClass.getResourceAsStream("/env/env.sl.yml")
      val lines = scala.io.Source
        .fromInputStream(stream)
        .getLines()
        .mkString("\n")
      val content = Utils.parseJinja(lines, Map("PROJECT_ID" -> "starlake-dev"))
      env = YamlSerializer.mapper.readValue(content, classOf[Env])
    }
  }
  new WithSettings() {
    "Load connections.sl.yml" should "succeed" in {
      val str = YamlSerializer.mapper.writeValueAsString(settings.appConfig.connections)
      println(str)
      val str2 = YamlSerializer.mapper.writeValueAsString(settings.appConfig)
      println(str2)
      assert(settings.appConfig.connections.size == 6)
    }
  }

}
