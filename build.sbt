import Dependencies._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.Version.Bump.Next
import xerial.sbt.Sonatype._

// require Java 8 for Spark 2 support
// javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"

lazy val scala212 = "2.12.16"

ThisBuild / crossScalaVersions := List(scala212)

organization := "ai.starlake"

organizationName := "starlake"

ThisBuild / scalaVersion := scala212

organizationHomepage := Some(url("https://github.com/starlake-ai/starlake"))

resolvers ++= Resolvers.allResolvers

libraryDependencies ++= {
  val (spark, jackson, esSpark, pureConfigs) = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => (spark_3d0_forScala_2d12, jackson212ForSpark3, esSpark212, pureConfig212)
      case Some((2, 11)) =>
        sys.env.getOrElse("COMET_HDP31", "false").toBoolean match {
          case false => (spark_2d4_forScala_2d11, jackson211ForSpark2, esSpark211, pureConfig211)
          case true  => (spark_2d4_forScala_2d11, jackson211ForSpark2Hdp31, esSpark211, pureConfig212)
        }
      case _ => throw new Exception(s"Invalid Scala Version")
    }
  }
  dependencies ++ spark ++ jackson ++ esSpark ++ pureConfigs ++ scalaReflection(scalaVersion.value)
}

dependencyOverrides := Seq(
  "org.scala-lang"         % "scala-library"             % scalaVersion.value,
  "org.scala-lang"         % "scala-reflect"             % scalaVersion.value,
  "org.scala-lang"         % "scala-compiler"            % scalaVersion.value
)

name := {
  val sparkNameSuffix = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => "3"
      case Some((2, 11)) => "2"
      case _             => throw new Exception(s"Invalid Scala Version")
    }
  }
  s"starlake-spark$sparkNameSuffix"
}

assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-assembly.jar"

Common.enableCometAliases

enablePlugins(Common.cometPlugins: _*)

Common.customSettings

// Builds a far JAR with embedded spark libraries and other provided libs.
// Can be useful for running YAML generation without having a spark distribution
commands += Command.command("assemblyWithSpark") { state =>
  """set assembly / fullClasspath := (Compile / fullClasspath).value""" :: "assembly" :: state
}

// Assembly
Test / fork := true

Test / envVars := Map("GOOGLE_CLOUD_PROJECT" -> "some-gcp-project")

Compile / assembly / artifact := {
  val art: Artifact = (Compile / assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(Compile / assembly / artifact, assembly)

assembly / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
  case PathList("META-INF", _ @_*) => MergeStrategy.discard
  case "reference.conf"            => MergeStrategy.concat
  case _                           => MergeStrategy.first
}

// Required by the Test container framework
Test / fork := true

assembly / assemblyExcludedJars := {
  val cp: Classpath = (assembly / fullClasspath).value
  val sortedCp = cp.sortBy(_.data.getName)
  sortedCp.foreach(x => println("->" + x.data.getName))
  sortedCp filter { jar =>
    val jarList = List(
      "netty-buffer-",
      "netty-common-",
      "arrow-memory-core-",
      "arrow-memory-netty-",
      "arrow-format-",
      "arrow-vector-",
      "commons-codec-",
      // "commons-compress-", // Because POI needs it
      "commons-logging-",
      "commons-math3-",
      "flatbuffers-java-",
      // "gson-", // because BigQuery needs com.google.gson.JsonParser.parseString(Ljava/lang/String;)
      //"guava-", // BigQuery needs com/google/common/base/MoreObjects (guava-20)
      "hadoop-client-api-",
      "hadoop-client-runtime-",
      "httpclient-",
      "httpcore-",
      "jackson-datatype-jsr310-",
      "json-",
      "jsr305-",
      "lz4-java-",
      // "protobuf-java-", // BigQuery needs com/google/protobuf/GeneratedMessageV3
      "scala-compiler-",
      "scala-library-",
      "scala-parser-combinators_",
      "scala-reflect-",
      "scala-xml_",
      "shapeless_",
      "slf4j-api-",
      "snappy-java-",
      "spark-tags_",
      "threeten-extra-",
      "xz-",
      "zstd-jni-"
    )
    if (jarList.exists(jar.data.getName.startsWith)) {
      println("Exclude ->" + jar.data.getName)
      true
    }
    else
      false
  }
}

assembly / assemblyShadeRules := Seq(
  // poi needs a newer version of commons-compress (> 1.17) than the one shipped with spark 2.4.X
  ShadeRule.rename("org.apache.commons.compress.**" -> "poiShade.commons.compress.@1").inAll,
//  ShadeRule.rename("shapeless.**" -> "shade.@0").inAll,
  //shade it or else writing to bigquery wont work because spark comes with an older version of google common.
  ShadeRule.rename("com.google.common.**" -> "shade.@0").inAll,
  ShadeRule.rename("com.google.gson.**" -> "shade.@0").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "shade.@0").inAll
)

// Publish
publishTo := {
  (
    sys.env.get("GCS_BUCKET_ARTEFACTS"),
    sys.env.getOrElse("RELEASE_SONATYPE", "true").toBoolean
  ) match {
    case (None, false) =>
      sonatypePublishToBundle.value
    case (None, true) => sonatypePublishToBundle.value
    case (Some(value), _) =>
      Some(GCSPublisher.forBucket(value, AccessRights.InheritBucket))
  }
}
// Disable scaladoc generation

Compile / doc / sources := Seq.empty

//Compile / packageDoc / publishArtifact := false

Compile / packageBin / publishArtifact := true

Compile / packageSrc / publishArtifact := true

// Do not disable checksum
publishLocal / checksums := Nil

// publish / checksums := Nil

// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "ai.starlake"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq(
  "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")
)

sonatypeProjectHosting := Some(
  GitHubHosting("starlake-ai", "starlake", "hayssam.saleh@starlake.ai")
)

// Release
releaseCrossBuild := true

releaseIgnoreUntrackedFiles := true

releaseProcess := Seq(
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
//  releaseStepCommand("+test"),
  setReleaseVersion,
  commitReleaseVersion, // forces to push dirty files
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

releaseCommitMessage := s"Release ${ReleasePlugin.runtimeVersion.value}"

releaseVersionBump := Next

developers := List(
  Developer(
    id = "hayssams",
    name = "Hayssam Saleh",
    email = "hayssam@saleh.fr",
    url = url("https://github.com/hayssams")
  ),
  Developer(
    id = "elarib",
    name = "Abdelhamide Elarib",
    email = "elarib.abdelhamide@gmail.com",
    url = url("https://github.com/elarib")
  ),
  Developer(
    id = "cchepelov",
    name = "Cyrille Chepelov",
    email = "cyrille@chepelov.org",
    url = url("https://github.com/cchepelov")
  ),
  Developer(
    id = "AmineSagaama",
    name = "Amine Sagaama",
    email = "amine.sagaama@gmail.com",
    url = url("https://github.com/AmineSagaama")
  ),
  Developer(
    id = "mhdqassir",
    name = "Mohamad Kassir",
    email = "mbkassir@gmail.com",
    url = url("https://github.com/mhdkassir")
  ),
  Developer(
    id = "mmenestret",
    name = "Martin Menestret",
    email = "martinmenestret@gmail.com",
    url = url("https://github.com/mmenestret")
  ),
  Developer(
    id = "pchalcol",
    name = "Patice Chalcol",
    email = "pchalcol@gmail.com",
    url = url("https://github.com/pchalcol")
  ),
  Developer(
    id = "zedach",
    name = "Mourad Dachraoui",
    email = "mourad.dachraoui@gmail.com",
    url = url("https://github.com/zedach")
  ),
  Developer(
    id = "seyguai",
    name = "Nicolas Boussuge",
    email = "nb.seyguai@gmail.com",
    url = url("https://github.com/seyguai")
  )
)

//assembly / logLevel := Level.Debug

// addCompilerPlugin("io.tryp" % "splain" % "0.5.8" cross CrossVersion.patch)
