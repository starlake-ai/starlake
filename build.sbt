import Dependencies._
import sbt.internal.util.complete.DefaultParsers
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.Version
import sbtrelease.Version.Bump.{Minor, Next}

name := "comet"

//val mavenLocal = "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
//resolvers += Resolver.mavenLocal

lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

crossScalaVersions := supportedScalaVersions

organization := "com.ebiznext"

organizationName := "Ebiznext"

scalaVersion := scala211

organizationHomepage := Some(url("http://www.ebiznext.com"))

libraryDependencies := {
  val spark = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, scalaMajor)) if scalaMajor == 12 => spark212
      case Some((2, scalaMajor)) if scalaMajor == 11 => spark211_240
    }
  }
  val jackson = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, scalaMajor)) if scalaMajor == 12 => jackson212
      case Some((2, scalaMajor)) if scalaMajor == 11 => jackson211
    }
  }
  
  dependencies ++ spark ++ jackson
}

Common.enableCometAliases

enablePlugins(Common.cometPlugins: _*)

Common.customSettings

Test / fork := true

artifact in (Compile, assembly) := {
  val art: Artifact = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

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

// Shade it or else bigquery wont work because spark comes with an older version of google common.
assemblyShadeRules in assembly := Seq(
  ShadeRule
    .rename(
      "com.google.cloud.hadoop.io.bigquery.**" -> "shadeio.@1",
      "com.google.common.**"                   -> "shadebase.@1"
    )
    .inAll
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
