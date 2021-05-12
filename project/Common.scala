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

import com.typesafe.sbt.GitPlugin.autoImport._
import com.typesafe.sbt.site.SiteScaladocPlugin
import com.typesafe.sbt.site.sphinx.SphinxPlugin
import com.typesafe.sbt.site.sphinx.SphinxPlugin.autoImport.Sphinx
import com.typesafe.sbt.{GitBranchPrompt, GitVersioning}
import org.scalafmt.sbt.ScalafmtPlugin.autoImport.scalafmtOnCompile
import sbt.Keys._
import sbt.{Def, _}
import sbtassembly.AssemblyKeys._
import sbtbuildinfo.BuildInfoPlugin

object Common {

  def enableCometAliases: Seq[Def.Setting[_]] =
    Seq(
      addCommandAlias("cd", "project"), // navigate the projects
      addCommandAlias("cc", ";clean;compile"), // clean and compile
      addCommandAlias("pl", ";clean;publishLocal"), // clean and publish locally
      addCommandAlias("pr", ";clean;publish"), // clean and publish globally
      addCommandAlias(
        "pld",
        ";clean;local:publishLocal;dockerComposeUp"
      ) // clean and publish/launch the docker environment
    ).flatten

  def cometPlugins: Seq[AutoPlugin] = Seq(
    GitVersioning,
    GitBranchPrompt,
    SphinxPlugin,
    SiteScaladocPlugin,
    BuildInfoPlugin
  )

  def gitSettings = Seq(
    git.useGitDescribe := true, {
      val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r
      git.gitTagToVersionNumber := {
        case VersionRegex(v, "")         => Some(v)
        case VersionRegex(v, "SNAPSHOT") => Some(s"$v-SNAPSHOT")
        case VersionRegex(v, s)          => Some(s"$v-$s-SNAPSHOT")
        case _                           => None
      }
    }
    //    git.gitTagToVersionNumber := { tag: String =>
//      if (tag matches "[0-9]+\\..*") Some(tag)
//      else None
//    }
  )

  def assemlySettings = Seq(
    assembly / test := {},
    Compile / mainClass := Some("com.ebiznext.comet.job.Main")
  )

  def customSettings: Seq[Def.Setting[_]] =
    Seq(
      scalacOptions ++= Seq(
        "-deprecation",
        "-feature",
        "-Xmacro-settings:materialize-derivations",
        "-Ywarn-unused-import",
        "-Xfatal-warnings"
      ),
      Test / testOptions ++= Seq(
        // show full stack traces and test case durations
        Tests.Argument("-oDF"),
        // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
        // -a Show stack traces a nd exception class name for AssertionErrors.
        Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
      ),
      Test / parallelExecution := false,
      scalafmtOnCompile := true
    ) ++ gitSettings ++ assemlySettings

}

object Versions {
  val sparkAvro = "4.0.0"
  val curator = "2.6.0"
  val spark2d4 = "2.4.7"
  val spark3d0 = "3.1.1"
  val scalatest = "3.2.8"
  val sparkXML = "0.12.0"
  val springBoot = "2.0.6.RELEASE"
  val typesafeConfig = "1.4.1"
  val scalaLogging = "3.9.3"
  val zookeeper = "3.4.6"
  val jets3t = "0.9.3"
  val hive = "3.1.0"
  val log4s = "1.3.3"
  val betterFiles = "3.9.1"
  val jackson211ForSpark2 = "2.6.7"
  val jackson212ForSpark3 = "2.10.0"
  val configs = "0.6.1"
  val esSpark211 = "7.8.1"
  val esSpark212 = "7.12.1"
  val scopt = "4.0.1"
  val sttp = "1.7.2"
  val gcs = "hadoop3-2.2.0"
  val hadoopbq = "hadoop3-1.0.0"
  val bq = "1.120.0"
  val hadoop = "3.3.0"
  val h2 = "1.4.200" // Test only
  val poi = "5.0.0"
  val scalate = "1.9.6"
  val akkaHttp = "10.1.14"
//  val akkaStream = "2.6.12"
  val akkaStream = "2.5.32"
  val kafkaClients = "2.8.0"
  val testContainers = "0.39.3"
  val jsqlparser = "4.0"
}

object Resolvers {

  val typeSafe = "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

  val allResolvers = Seq(
    typeSafe
  )

  val googleCloudBigDataMavenRepo = "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss"

}
