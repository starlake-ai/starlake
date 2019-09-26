import Dependencies._
import sbtrelease.Version
import ReleaseTransformations._
import sbt.internal.util.complete.DefaultParsers

name := "comet"


//val mavenLocal = "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
//resolvers += Resolver.mavenLocal

lazy val scala212 = "2.12.8"
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
  dependencies ++ spark
}

unmanagedJars in Compile += file("lib/gcs-connector-hadoop2-latest.jar")

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
    case None => None
    case Some(value) => Some(GCSPublisher.forBucket(value, AccessRights.InheritBucket))
  }
}


// Release

releaseCrossBuild :=  true

releaseNextVersion := { ver =>
  Version(ver) match {
    case Some(v @ Version(_, Seq(_, 0), _)) => v.bump(sbtrelease.Version.Bump.Minor).asSnapshot.string
    case Some(v @ Version(_, Seq(_, _), _)) => v.bump(sbtrelease.Version.Bump.Bugfix).asSnapshot.string
    case None                               => sys.error("No version detected")
  }
}

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  releaseStepCommand("+test"),
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("+publish"),
  // releaseStepCommand("universal:publish"), // publish jars and tgz archives in the snapshot or release repository
  setNextVersion,
  commitNextVersion,
  pushChanges
)

releaseCommitMessage := s"Add CLoud Build ${ReleasePlugin.runtimeVersion.value}"

val writeNextVersion =
  Command("writeNextVersion")(_ => DefaultParsers.SpaceClass ~> DefaultParsers.NotQuoted)((st, str) => {
    Version(str) match {
      case Some(ver) =>
        val verStr = ver.string
        val versionFile = Project.extract(st).get(releaseVersionFile)
        val useGlobal = Project.extract(st).get(releaseUseGlobalVersion)
        val formattedVer = (if (useGlobal) globalVersionString else versionString) format verStr
        IO.writeLines(versionFile, Seq(formattedVer))
        val refreshedSt = reapply(Seq(
          if (useGlobal) version in ThisBuild := verStr
          else version := verStr
        ),
          st)

        commitNextVersion.action(refreshedSt)

      case _ => sys.error("Input version does not follow semver")
    }
  })
