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

import sbt._

object Dependencies {

  val jacksonExclusions = Seq(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "org.codehaus.jackson")
  )

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest,
    "org.scalatest" %% "scalatest" % Versions.scalatest % "test"
  )

  val betterfiles = Seq("com.github.pathikrit" %% "better-files" % Versions.betterFiles)

  val logging = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
  )

  val typedConfigs = Seq("com.github.kxbmap" %% "configs" % Versions.configs)

  val okhttp = Seq("com.squareup.okhttp3" % "okhttp" % Versions.okhttp)

  val jackson = Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
    "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson,
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson,
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions.jackson
  )

  val spark211 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark211 % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.spark211 % "provided",
    "org.apache.spark" %% "spark-hive" % Versions.spark211 % "provided",
    "org.apache.spark" %% "spark-mllib" % Versions.spark211 % "provided"
  )

  val spark212 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark212 % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.spark212 % "provided",
    "org.apache.spark" %% "spark-hive" % Versions.spark212 % "provided",
    "org.apache.spark" %% "spark-mllib" % Versions.spark212 % "provided"
  )


  val esHadoop = Seq(
    "org.elasticsearch" % "elasticsearch-hadoop" % Versions.esHadoop
  )

  val scopt = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  val sttp = Seq ("com.softwaremill.sttp" %% "core" % Versions.sttp)

  val dependencies = logging ++ typedConfigs ++ spark ++ okhttp ++ betterfiles ++ jackson ++ scalaTest ++ scopt ++ esHadoop ++ sttp
}
