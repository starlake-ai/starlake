import Dependencies._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.Version.Bump.Next

import scala.util.matching.Regex

//val mavenLocal = "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
//resolvers += Resolver.mavenLocal

lazy val scala212 = "2.12.12"

// lazy val scala211 = "2.11.12"

lazy val sparkVersion = sys.env.getOrElse("COMET_SPARK_VERSION", "3.0.1")

val sparkVersionPattern: Regex = "(\\d+).(\\d+).(\\d+)".r

val sparkPatternMatch = sparkVersionPattern
  .findFirstMatchIn(sparkVersion)
  .getOrElse(throw new Exception(s"Invalid Spark Version $sparkVersion"))
val sparkMajor = sparkPatternMatch.group(1)
val sparkMinor = sparkPatternMatch.group(2)

lazy val supportedScalaVersions = sparkMajor match {
  case "3" => List(scala212)
  case "2" => List(scala212) // scala211
  case _   => throw new Exception(s"Invalid Spark Major Version $sparkMajor")
}

crossScalaVersions := supportedScalaVersions

organization := "com.ebiznext"

organizationName := "Ebiznext"

scalaVersion := scala212

organizationHomepage := Some(url("http://www.ebiznext.com"))

libraryDependencies ++= {
  val (spark, jackson) = {
    System.out.println(s"sparkMajor=$sparkMajor")
    sparkMajor match {
      case "3" =>
            (spark_3d0_forScala_2d12, jackson312)
      case "2" =>
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, scalaMinor)) if scalaMinor == 12 => (spark_2d4_forScala_2d12, jackson212)
          case Some((2, scalaMinor)) if scalaMinor == 11 =>
            sparkMinor match {
              case "1" => (spark_2d1_forScala_2d11, jackson211)
              case _   => (spark_2d4_forScala_2d11, jackson211)
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

releaseCrossBuild := false

releaseIgnoreUntrackedFiles := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  releaseStepCommand("+test"),
  setReleaseVersion,
  commitReleaseVersion, // forces to push dirty files
  tagRelease,
  // releaseStepCommand("+publish"),
  // releaseStepCommand("universal:publish"), // publish jars and tgz archives in the snapshot or release repository
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

releaseCommitMessage := s"Add Cloud Build ${ReleasePlugin.runtimeVersion.value}"

releaseVersionBump := Next

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x                             => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp: Classpath = (fullClasspath in assembly).value
  cp.foreach(x => println("->" + x.data.getName))
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
sonatypeProjectHosting := Some(
  GitHubHosting("ebiznext", "comet-data-pipeline", "hayssam.saleh@ebiznext.com")
)

developers := List(
  Developer(
    id = "hayssams",
    name = "Hayssam Saleh",
    email = "hayssam.saleh@ebiznext.com",
    url = url("https://www.ebiznext.com")
  )
)

//logLevel in assembly := Level.Debug
