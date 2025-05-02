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
package ai.starlake.schema.generator

/** @param outputDir
  *   Path for saving the resulting DAG file(s).
  * @param clean
  *   Clean Resulting DAG file output first ?
  * @param tasks
  *   Whether to generate DAG file(s) for tasks or not
  */
case class DagGenerateConfig(
  outputDir: Option[String] = None,
  clean: Boolean = false,
  tags: Seq[String] = Nil,
  domains: Boolean = false,
  tasks: Boolean = false,
  masterProjectId: Option[String] = None,
  masterProjectName: Option[String] = None
)
