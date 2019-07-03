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

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.Encryption
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

/**
  * How (the attribute should be transformed at ingestion time ?
  *
  * @param value algorithm to use : NONE, HIDE, MD5, SHA1, SHA256, SHA512, AES
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[PrivacyLevelDeserializer])
sealed case class PrivacyLevel(value: String) {
  override def toString: String = value

  def encrypt(s: String): String = PrivacyLevel.all(value)._1.encrypt(s)

}

object PrivacyLevel {
  lazy val all = Settings.comet.privacy.options.asScala.map { case (k, objName) =>
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(objName)
    val obj: universe.ModuleMirror = runtimeMirror.reflectModule(module)
    val encryption = obj.instance.asInstanceOf[Encryption]
    (k.toUpperCase(), (encryption, new PrivacyLevel(k.toUpperCase())))
  }

  // Improve Scan performance
  lazy val None = all("NONE")._2

  def fromString(value: String): PrivacyLevel = all(value.toUpperCase())._2
}

class PrivacyLevelDeserializer extends JsonDeserializer[PrivacyLevel] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrivacyLevel = {
    val value = jp.readValueAs[String](classOf[String])
    PrivacyLevel.fromString(value)
  }
}
