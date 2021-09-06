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

import sbt.{ExclusionRule, _}

object Dependencies {

  def scalaReflection(scalaVersion: String) =
    Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion
    )

  val jacksonExclusions = Seq(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "com.fasterxml.jackson.databind"),
    ExclusionRule(organization = "com.fasterxml.jackson.jaxrs"),
    ExclusionRule(organization = "com.fasterxml.jackson.module")
  )

  val sparkExclusions = Seq(
    ExclusionRule(organization = "org.apache.spark")
  )

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest % "test"
  )

  val betterfiles = Seq("com.github.pathikrit" %% "better-files" % Versions.betterFiles)

  val logging = Seq(
    "com.typesafe" % "config" % Versions.typesafeConfig,
    "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
  )

  val typedConfigs = Seq("com.github.kxbmap" %% "configs" % Versions.configs)

  val jackson211ForSpark2 = Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson211ForSpark2 % "provided",
    "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson211ForSpark2 % "provided",
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson211ForSpark2 % "provided",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson211ForSpark2 % "provided",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions.jackson211ForSpark2 % "provided"
  )

  val jackson212ForSpark3 = Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson212ForSpark3 % "provided",
    "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson212ForSpark3 % "provided",
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson212ForSpark3 % "provided",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson212ForSpark3 % "provided",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions.jackson212ForSpark3 % "provided"
  )

  val spark_2d4_forScala_2d11 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark2d4 % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "org.apache.spark" %% "spark-sql" % Versions.spark2d4 % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "org.apache.spark" %% "spark-hive" % Versions.spark2d4 % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "org.apache.spark" %% "spark-mllib" % Versions.spark2d4 % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "com.databricks" %% "spark-xml" % Versions.sparkXML,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark2d4 % "provided"
  )

  val spark_3d0_forScala_2d12 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark3d0 % "provided" exclude ("com.google.guava", "guava") excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-sql" % Versions.spark3d0 % "provided" exclude ("com.google.guava", "guava") excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-hive" % Versions.spark3d0 % "provided" exclude ("com.google.guava", "guava") excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-mllib" % Versions.spark3d0 % "provided" exclude ("com.google.guava", "guava") excludeAll (jacksonExclusions: _*),
    "com.databricks" %% "spark-xml" % Versions.sparkXML,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark3d0 % "provided"
  )

  val gcsConnectorShadedJar =
    s"${Resolvers.googleCloudBigDataMavenRepo}/gcs-connector/${Versions.gcs}/gcs-connector-${Versions.gcs}-shaded.jar"

  val gcpBigQueryConnectorShadedJar =
    s"${Resolvers.googleCloudBigDataMavenRepo}/bigquery-connector/${Versions.hadoopbq}/bigquery-connector-${Versions.hadoopbq}-shaded.jar"

  val gcp = Seq(
    "com.google.cloud.bigdataoss" % "gcs-connector" % Versions.gcs from gcsConnectorShadedJar exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri") excludeAll (jacksonExclusions: _*) classifier "shaded",
    "com.google.cloud.bigdataoss" % "bigquery-connector" % Versions.hadoopbq from gcpBigQueryConnectorShadedJar exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri") excludeAll (jacksonExclusions: _*) classifier "shaded",
    "com.google.cloud" % "google-cloud-bigquery" % Versions.bq exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri") excludeAll (jacksonExclusions: _*),
    // see https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/36
    // Add the jar file to spark dependencies
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.22.0" % "provided" excludeAll (jacksonExclusions: _*),
    "com.google.cloud" % "google-cloud-nio" % "0.123.9" exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri") excludeAll (jacksonExclusions: _*) classifier "shaded"
  )

  val esSpark211 = Seq(
    "org.elasticsearch" %% "elasticsearch-spark-20" % Versions.esSpark211 exclude ("com.google.guava", "guava") excludeAll ((sparkExclusions ++ jacksonExclusions): _*),
    "com.dimafeng" %% "testcontainers-scala-elasticsearch" % Versions.testContainers % Test
  )

  val esSpark212 = Seq(
    "org.elasticsearch" %% "elasticsearch-spark-30" % Versions.esSpark212 exclude ("com.google.guava", "guava") excludeAll ((sparkExclusions ++ jacksonExclusions): _*),
    "com.dimafeng" %% "testcontainers-scala-elasticsearch" % Versions.testContainers % Test
  )

  val scopt = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  val sttp = Seq(
    "com.softwaremill.sttp" %% "core" % Versions.sttp
  )

  // We need here to remove any reference to hadoop 3
  val atlas = Seq(
    //"org.apache.atlas" % "apache-atlas" % "2.0.0" pomOnly(),
    "org.apache.atlas" % "atlas-intg" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm") exclude ("com.google.guava", "guava"),
    "org.apache.atlas" % "atlas-client-common" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm") exclude ("com.google.guava", "guava"),
    //"org.apache.atlas" % "atlas-client" % "2.0.0" pomOnly(),
    "org.apache.atlas" % "atlas-common" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm") exclude ("com.google.guava", "guava"),
    "org.apache.atlas" % "atlas-client-v2" % "2.0.0" excludeAll (jacksonExclusions: _*) exclude ("asm", "asm") exclude ("com.google.guava", "guava")
  )

  val azure = Seq(
    "org.apache.hadoop" % "hadoop-azure" % "3.3.1" % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "com.microsoft.azure" % "azure-storage" % "8.6.6" % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava")
  )

  val hadoop = Seq(
    "org.apache.hadoop" % "hadoop-common" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "org.apache.hadoop" % "hadoop-hdfs" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "org.apache.hadoop" % "hadoop-yarn-client" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "org.apache.hadoop" % "hadoop-mapreduce-client-app" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava"),
    "org.apache.hadoop" % "hadoop-client" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude ("com.google.guava", "guava")
  )

  val excelClientApi = Seq(
    "org.apache.poi" % "poi-ooxml" % Versions.poi
  )

  val h2 = Seq(
    "com.h2database" % "h2" % Versions.h2 % Test
  )

  val scalate = Seq(
    "org.scalatra.scalate" %% "scalate-core" % Versions.scalate
  )

  val akkaHttp = Seq(
    "com.typesafe.akka" %% "akka-http" % Versions.akkaHttp
  )

  val akkaStream = Seq(
    "com.typesafe.akka" %% "akka-stream" % Versions.akkaStream
  )

  val kafkaClients = Seq(
    "org.apache.kafka" % "kafka-clients" % Versions.kafkaClients,
    "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.testContainers % Test,
    "com.dimafeng" %% "testcontainers-scala-kafka" % Versions.testContainers % Test
  )

  val graphviz = Seq(
    "com.github.jsqlparser" % "jsqlparser" % Versions.jsqlparser
  )

  val dependencies =
    scalate ++ logging ++ typedConfigs ++ betterfiles ++ scalaTest ++ scopt ++ hadoop ++
    sttp ++ gcp ++ azure ++ h2 ++ excelClientApi ++ akkaHttp ++ akkaStream ++ kafkaClients ++ graphviz // ++ atlas
}
