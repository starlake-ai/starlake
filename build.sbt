import Dependencies._
import sbt.internal.util.complete.DefaultParsers
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.Version
import sbtrelease.Version.Bump.Next

import scala.util.matching.Regex

//val mavenLocal = "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
//resolvers += Resolver.mavenLocal

lazy val scala212 = "2.12.12"

lazy val scala211 = "2.11.12"

lazy val sparkVersion = sys.env.getOrElse("COMET_SPARK_VERSION", "3.0.0")

val nothing = println("DEBUG: spark version COMET_SPARK_VERSION received from env vars (Travis) = " + sparkVersion)

lazy val supportedScalaVersions = List(scala212, scala211)

crossScalaVersions := supportedScalaVersions

organization := "com.ebiznext"

organizationName := "Ebiznext"

scalaVersion := scala212

organizationHomepage := Some(url("http://www.ebiznext.com"))

val sparkVersionPattern: Regex = "(\\d+).(\\d+).(\\d+)".r
val sparkPatternMatch = sparkVersionPattern
  .findFirstMatchIn(sparkVersion)
  .getOrElse(throw new Exception(s"Invalid Spark Version $sparkVersion"))
val sparkMajor = sparkPatternMatch.group(1)
val sparkMinor = sparkPatternMatch.group(2)


libraryDependencies ++= {
  val (spark, jackson) = {
    sparkMajor match {
      case "3" =>
        (spark_3d0_forScala_2d12, jackson312)
      case "2" =>
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, scalaMinor)) if scalaMinor == 12 => (spark_2d4_forScala_2d12, jackson212)
          case Some((2, scalaMinor)) if scalaMinor == 11 =>
            sparkMinor match {
              case "1" => (spark_2d1_forScala_2d11, jackson211)
              case _ => (spark_2d4_forScala_2d11, jackson211)
            }
        }
    }
  }
  dependencies ++ spark ++ jackson ++ scalaReflection(scalaVersion.value)
}

name := s"comet-spark${sparkMajor}"

assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-assembly.jar"

/*
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-spark" + sparkMajor +   + "_" + sv.binary "-" + module.revision + "." + artifact.extension
}
 */
// assemblyJarName in assembly := s"comet-spark-${sparkVersion}_${scalaVersion.value}-assembly.jar"

Common.enableCometAliases

enablePlugins(Common.cometPlugins: _*)

Common.customSettings

Test / fork := true
envVars in Test := Map("GOOGLE_CLOUD_PROJECT" -> "some-gcp-project")

artifact in (Compile, assembly) := {
  val art: Artifact = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

// Builds a far JAR with embedded spark libraries and other provided libs.
// Can be useful for running YAML generation without having a spark distribution
commands += Command.command("assemblyWithSpark") { state =>
  """set assembly / fullClasspath := (Compile / fullClasspath).value""" :: "assembly" :: state
}

publishTo in ThisBuild := {
  sys.env.get("GCS_BUCKET_ARTEFACTS") match {
    case None        => sonatypePublishToBundle.value
    case Some(value) => Some(GCSPublisher.forBucket(value, AccessRights.InheritBucket))
  }
}

// Release

releaseCrossBuild := true

releaseNextVersion := { ver =>
  Version(ver) match {
    case Some(v @ Version(_, Seq(_, 0), _)) =>
      v.bump(sbtrelease.Version.Bump.Minor).asSnapshot.string
    case Some(v @ Version(_, Seq(_, _), _)) =>
      v.bump(sbtrelease.Version.Bump.Bugfix).asSnapshot.string
    case None => sys.error("No version detected")
  }
}

releaseIgnoreUntrackedFiles := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  releaseStepCommand("+test"),
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  // releaseStepCommand("+publish"),
  // releaseStepCommand("universal:publish"), // publish jars and tgz archives in the snapshot or release repository
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

releaseCommitMessage := s"Add CLoud Build ${ReleasePlugin.runtimeVersion.value}"

releaseVersionBump := Next

val writeNextVersion =
  Command("writeNextVersion")(_ => DefaultParsers.SpaceClass ~> DefaultParsers.NotQuoted)(
    (st, str) => {
      Version(str) match {
        case Some(ver) =>
          val verStr = ver.string
          val versionFile = Project.extract(st).get(releaseVersionFile)
          val useGlobal = Project.extract(st).get(releaseUseGlobalVersion)
          val formattedVer = (if (useGlobal) globalVersionString else versionString) format verStr
          IO.writeLines(versionFile, Seq(formattedVer))
          val refreshedSt = reapply(
            Seq(
              if (useGlobal) version in ThisBuild := verStr
              else version := verStr
            ),
            st
          )

          commitNextVersion.action(refreshedSt)

        case _ => sys.error("Input version does not follow semver")
      }
    }
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp: Classpath = (fullClasspath in assembly).value
  cp.foreach(x => println("->"+x.data.getName))
  //cp filter {_.data.getName.matches("hadoop-.*-2.6.5.jar")}
  Nil
}


assemblyShadeRules in assembly := Seq(
  // poi needs a newer version of commons-compress (> 1.17) than the one shipped with spark (1.4)
  ShadeRule.rename("org.apache.commons.compress.**" -> "poiShade.commons.compress.@1").inAll,
  //shade it or else writing to bigquery wont work because spark comes with an older version of google common.
  ShadeRule.rename("com.google.common.**" -> "shade.@0").inAll
)

// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "com.ebiznext"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

// Where is the source code hosted: GitHub or GitLab?
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("ebiznext", "comet-data-pipeline", "hayssam.saleh@ebiznext.com"))

developers := List(
  Developer(id="hayssams", name="Hayssam Saleh", email="hayssam.saleh@ebiznext.com", url=url("https://www.ebiznext.com"))
)


//logLevel in assembly := Level.Debug
