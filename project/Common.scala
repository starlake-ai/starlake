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

import com.github.sbt.git.GitPlugin.autoImport._
import com.typesafe.sbt.site.SiteScaladocPlugin
import com.typesafe.sbt.site.sphinx.SphinxPlugin
import com.github.sbt.git.{GitBranchPrompt, GitVersioning}
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
    Compile / mainClass := Some("ai.starlake.job.Main")
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

object Resolvers {

  val typeSafe = "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"
  val confluent = "Confluent repository" at "https://packages.confluent.io/maven/"

  val allResolvers = Seq(typeSafe, confluent)

  val googleCloudBigDataMavenRepo = "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss"

}
