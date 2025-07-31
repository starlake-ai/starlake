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
package ai.starlake.job.infer

import ai.starlake.schema.model.{Format, WriteMode}
import better.files.File

import java.nio.charset.{Charset, StandardCharsets}
import java.util.regex.Pattern

case class InferSchemaConfig(
  domainName: String = "",
  schemaName: String = "",
  inputPath: String = "",
  outputDir: Option[String] = None,
  format: Option[Format] = None,
  write: Option[WriteMode] = None,
  rowTag: Option[String] = None,
  clean: Boolean = false,
  encoding: Charset = StandardCharsets.UTF_8,
  variant: Option[Boolean] = None
) {
  def extractTableNameAndWriteMode(): (String, WriteMode) = {
    val file: File = File(inputPath)
    val fileNameWithoutExt = file.nameWithoutExtension(includeAll = true)
    val pattern = Pattern.compile("[._-][0-9]")
    val matcher = pattern.matcher(fileNameWithoutExt)
    if (matcher.find()) {
      val matchedChar = fileNameWithoutExt.charAt(matcher.start())
      val lastIndex = fileNameWithoutExt.lastIndexOf(matchedChar)
      val name = fileNameWithoutExt.substring(0, lastIndex)
      val deltaPart = fileNameWithoutExt.substring(lastIndex + 1)
      if (deltaPart.nonEmpty && deltaPart(0).isDigit) {
        (name.replaceAll("\\.", "_").replaceAll("-", "_"), WriteMode.APPEND)
      } else {
        (fileNameWithoutExt, WriteMode.OVERWRITE)
      }
    } else {
      val name = fileNameWithoutExt
      val write = WriteMode.OVERWRITE
      (name, write)
    }
  }

}
