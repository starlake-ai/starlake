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
package ai.starlake.extract

case class ExtractDataConfig(
  extractConfig: String = "",
  outputDir: Option[String] = None,
  limit: Int = 0,
  numPartitions: Int = 1,
  parallelism: Option[Int] = None,
  fullExport: Boolean = true,
  ifExtractedBefore: Option[Long] = None,
  cleanOnExtract: Boolean = false,
  includeSchemas: Seq[String] = Seq.empty,
  excludeSchemas: Seq[String] = Seq.empty,
  includeTables: Seq[String] = Seq.empty,
  excludeTables: Seq[String] = Seq.empty
)
