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

import sbt.{File, IntegrationTest, Project, file}

trait Definition {

  def play(name: String): Project = play(name, file(name))

  def play(name: String, src: File): Project =
    sbt
      .Project(id = name, base = src)

  /**
    * Creates a library module
    *
    * @param name the name of the module, will be used as the module's root folder name
    * @return the module's `Project`
    */
  def library(name: String): Project = library(name, file(name))

  /**
    * Creates a library sub project
    *
    * @param name the name of the project
    * @param src  the module's root folder
    * @return the module's `Project`
    */
  def library(name: String, src: File): Project =
    sbt
      .Project(id = name, base = src)
      .configs(IntegrationTest)

}
