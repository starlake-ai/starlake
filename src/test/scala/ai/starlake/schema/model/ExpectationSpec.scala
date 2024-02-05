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
import ai.starlake.utils.Utils

class ExpectationSpec extends TestHelper {
  new WithSettings() {
    "Parse expectations" should "succeed" in {
      val macros =
        """
          |{% macro count_max_occurrences(col, table='sl_this') %}
          |    SELECT max(cnt)
          |    FROM (SELECT {{ col }}, count(*) as cnt FROM {{ table }}
          |    GROUP BY {{ col }})
          |{% endmacro %}
          |""".stripMargin
      val expectationQuery =
        """
          |{{ count_max_occurrences('id') }}
          |""".stripMargin

      val toParse =
        s"""
           |$macros
           |$expectationQuery
           |""".stripMargin
      println(toParse)
      val parsed = Utils.parseJinja(toParse, Map.empty[String, Any]).trim
      println(parsed)
      parsed should be(
        s"""
        |SELECT max(cnt)
        |    FROM (SELECT id, count(*) as cnt FROM sl_this
        |    GROUP BY id
        |    HAVING cnt > 1)""".stripMargin.trim
      )
    }
  }
}
