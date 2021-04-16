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

package com.ebiznext.comet.schema.model

import java.util.Locale
import com.ebiznext.comet.config.Settings
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/** How (the attribute should be transformed at ingestion time ?
  *
  * @param value algorithm to use : NONE, HIDE, MD5, SHA1, SHA256, SHA512, AES
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[PrivacyLevelDeserializer])
sealed case class PrivacyLevel(value: String) {
  override def toString: String = value

  def crypt(s: String, colMap: Map[String, Option[String]])(implicit settings: Settings): String = {
    val ((privacyAlgo, privacyParams), _) = settings.allPrivacyLevels(value)
    privacyAlgo.crypt(s, colMap, privacyParams)
  }

}

object PrivacyLevel {

  val None: PrivacyLevel = PrivacyLevel("NONE")
}

class PrivacyLevelDeserializer extends JsonDeserializer[PrivacyLevel] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrivacyLevel = {
    val value = jp.readValueAs[String](classOf[String])
    PrivacyLevel(
      value.toUpperCase(Locale.ROOT)
    ) /* TODO: prior to 2020-02-25 we enforced a valid PrivacyLevel at deser time */
  }
}
