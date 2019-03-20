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

import sbt.{Def, _}
import sbt.Keys._
import com.typesafe.sbt.GitPlugin.autoImport._
import com.typesafe.sbt.site.SiteScaladocPlugin
import com.typesafe.sbt.site.sphinx.SphinxPlugin
import sbtassembly.AssemblyKeys._
import com.typesafe.sbt.site.sphinx.SphinxPlugin.autoImport.Sphinx
import org.scalafmt.sbt.ScalafmtPlugin.autoImport.scalafmtOnCompile

object Common {

  def enableCometAliases: Seq[Def.Setting[_]] =
    Seq(
      addCommandAlias("cd", "project"), // navigate the projects
      addCommandAlias("cc", ";clean;compile"), // clean and compile
      addCommandAlias("pl", ";clean;publishLocal"), // clean and publish locally
      addCommandAlias("pr", ";clean;publish"), // clean and publish globally
      addCommandAlias("pld", ";clean;local:publishLocal;dockerComposeUp") // clean and publish/launch the docker environment
    ).flatten

  def cometPlugins: Seq[AutoPlugin] = Seq(
    com.typesafe.sbt.GitVersioning,
    SphinxPlugin,
    SiteScaladocPlugin
  )

  def gitSettings = Seq(
    git.useGitDescribe := true,
    git.gitTagToVersionNumber := { tag: String =>
      if (tag matches "[0-9]+\\..*") Some(tag)
      else None
    }
  )

  def assemlySettings = Seq(
    test in assembly := {},
    mainClass in Compile := Some("com.ebiznext.comet.job.Main")
  )

  def docsSettings = Seq(
    sourceDirectory in Sphinx := baseDirectory.value / "docs"
  )

  def customSettings: Seq[Def.Setting[_]] =
    Seq(
      scalacOptions ++= Seq(
        "-Xmacro-settings:materialize-derivations",
      ),
      testOptions in Test ++= Seq(
        // show full stack traces and test case durations
        Tests.Argument("-oDF"),
        // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
        // -a Show stack traces a nd exception class name for AssertionErrors.
        Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
      ),
      version := "0.1",
      parallelExecution in Test := false,
      scalafmtOnCompile := true
    ) ++ gitSettings ++ assemlySettings ++ docsSettings

}

object Versions {
  val sparkAvro = "4.0.0"
  val hadoop = "2.7.3"
  val spark = "2.4.0"
  val curator = "2.6.0"
  val scalatest = "3.0.5"
  val springBoot = "2.0.6.RELEASE"
  val typesafeConfig = "1.2.1"
  val okhttp = "3.11.0"
  val scalaLogging = "3.9.0"
  val logback = "1.2.3"
  val slf4j = "1.7.21"
  val zookeeper = "3.4.6"
  val jets3t = "0.9.3"
  val hive = "3.1.0"
  val log4s = "1.3.3"
  val betterFiles = "3.6.0"
  val jackson = "2.6.5"
  val configs = "0.4.4"
  val esHadoop = "6.6.2"
  val scopt = "4.0.0-RC2"
  val sttp = "1.5.11"
}

object Resolvers {

  val typeSafe = "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

  val allResolvers = Seq(
    typeSafe
  )

}
