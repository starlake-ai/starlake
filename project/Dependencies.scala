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
    ExclusionRule(organization = "com.fasterxml.jackson.core")
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
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson,
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions.jackson
  )

  val spark211 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark211 % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.spark211 % "provided",
    "org.apache.spark" %% "spark-hive" % Versions.spark211 % "provided",
    "org.apache.spark" %% "spark-mllib" % Versions.spark211 % "provided"
  )

  val spark211_240 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark211_240 % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.spark211_240 % "provided",
    "org.apache.spark" %% "spark-hive" % Versions.spark211_240 % "provided",
    "org.apache.spark" %% "spark-mllib" % Versions.spark211_240 % "provided"
  )

  val spark212 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark212 % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.spark212 % "provided",
    "org.apache.spark" %% "spark-hive" % Versions.spark212 % "provided",
    "org.apache.spark" %% "spark-mllib" % Versions.spark212 % "provided"
  )

  val gcp = Seq(
    "com.google.cloud.bigdataoss" % "gcs-connector" % Versions.gcs exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri"),
    "com.google.cloud.bigdataoss" % "bigquery-connector" % Versions.hadoopbq exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri"),
    "com.google.cloud" % "google-cloud-bigquery" % Versions.bq exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri"),
    // A more recent version of guava is requierd for the big query connector
    "com.google.guava" % "guava" % "28.1-jre",
    // We include the files below because guava above introduce static constructors which break previous hadoop versions
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.3" exclude ("javax.servlet", "servlet-api") exclude ("aopalliance", "aopalliance") exclude ("org.sonatype.sisu.inject", "cglib"),
    "org.apache.hadoop" % "hadoop-common" % "2.7.3" exclude ("commons-beanutils", "commons-beanutils") exclude ("commons-beanutils", "commons-beanutils-core") exclude ("javax.servlet", "servlet-api") exclude ("aopalliance", "aopalliance") exclude ("org.sonatype.sisu.inject", "cglib"),
    "org.xerial.snappy" % "snappy-java" % "1.1.7.3"
  )

  val esHadoop = Seq(
    "org.elasticsearch" % "elasticsearch-hadoop" % Versions.esHadoop exclude ("org.apache.hadoop", "hadoop-common")
  )

  val scopt = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  val sttp = Seq("com.softwaremill.sttp" %% "core" % Versions.sttp)

  val atlas = Seq(
    //"org.apache.atlas" % "apache-atlas" % "2.0.0" pomOnly(),
    "org.apache.atlas" % "atlas-intg" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm"),
    "org.apache.atlas" % "atlas-client-common" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm"),
    //"org.apache.atlas" % "atlas-client" % "2.0.0" pomOnly(),
    "org.apache.atlas" % "atlas-common" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm"),
    "org.apache.atlas" % "atlas-client-v2" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm")
  )

  val dependencies = logging ++ typedConfigs ++ okhttp ++ betterfiles ++ jackson ++ scalaTest ++ scopt ++ esHadoop ++ sttp ++ gcp ++ atlas
}
