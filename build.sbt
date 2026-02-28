import Dependencies.*
import sbt.Tests.{Group, SubProcess}
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations.*
import sbtrelease.Version.Bump.Next
import xerial.sbt.Sonatype.*

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

lazy val javacCompilerVersion = "17"


javacOptions ++= Seq(
  "-source", javacCompilerVersion,
  "-target", javacCompilerVersion,
  "-Xlint"
)

updateOptions := updateOptions.value.withLatestSnapshots(true)

Test / javaOptions ++= Seq("-Dfile.encoding=UTF-8")

val testJavaOptions = {
  val heapSize = sys.env.get("HEAP_SIZE").getOrElse("4g")
  val extraTestJavaArgs = Seq("-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED").mkString(" ")
  s"-Xmx$heapSize -Xss4m -XX:ReservedCodeCacheSize=128m -Dfile.encoding=UTF-8 $extraTestJavaArgs"
    .split(" ").toSeq
}

Test / javaOptions ++= testJavaOptions

// lazy val scala212 = "2.12.20"

lazy val scala213 = "2.13.18"
lazy val scala3 = "3.3.1"

lazy val supportedScalaVersions = Seq(scala213)


crossScalaVersions := supportedScalaVersions

organization := "ai.starlake"

organizationName := "starlake"

scalaVersion := scala213 // scala213 scala212

organizationHomepage := Some(url("https://github.com/starlake-ai/starlake"))

resolvers ++= Resolvers.allResolvers

libraryDependencies ++= {
  val versionSpecificLibs = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => scalaCompat ++ scala213LibsOnly
      case _ => throw new Exception(s"Invalid Scala Version")
    }
  }
  dependencies(isSnapshot.value) ++ spark3 ++
    jacksonForSpark3 ++ // esSpark212 ++ (exclude elasticsearch until spark 4 is supported
    pureConfig ++ scalaReflection(scalaVersion.value) ++
    versionSpecificLibs
}

dependencyOverrides := Seq(
  "com.google.protobuf"                % "protobuf-java"             % "3.25.8",
  "com.google.protobuf"                % "protobuf-java"             % "3.25.8",
  "org.scala-lang"                    % "scala-library"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-reflect"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-compiler"            % scalaVersion.value,
  "com.google.guava"                  %  "guava"                    % "33.5.0-jre", // required by jinjava 2.7.3
  "com.fasterxml.jackson.dataformat"  % "jackson-dataformat-csv"    % Versions.jacksonForSpark3
)

name := "starlake-core"


Common.enableStarlakeAliases

enablePlugins(Common.starlakePlugins: _*)

buildInfoPackage := "ai.starlake.buildinfo"


scalacOptions ++= {
  val extractOptions = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Xfatal-warnings")
      case Some((2, 13)) =>  Seq()
      case _ => throw new Exception(s"Invalid Scala Version")
    }
  }
  Seq(
    "-deprecation",
    "-feature",
    "-Xmacro-settings:materialize-derivations",
    "-Ywarn-unused:imports",
    "-Xsource:3"  ) ++ extractOptions

}


Common.customSettings

// Builds a far JAR with embedded spark libraries and other provided libs.
// Can be useful for running YAML generation without having a spark distribution
commands += Command.command("assemblyWithSpark") { state =>
  """set assembly / fullClasspath := (Compile / fullClasspath).value""" :: "assembly" :: state
}

assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-assembly.jar"

Compile / assembly / artifact := {
  val art: Artifact = (Compile / packageBin / artifact).value
  art.withClassifier(Some("assembly"))
}

// Assembly
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

excludeDependencies ++= Seq(
  ExclusionRule("org.javassist", "javassist")
)

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
      //"commons-compress-", // Because POI needs it
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
      "json-2",
      "jsr305-",
      "lz4-java-",
      // "protobuf-java-", // BigQuery needs com/google/protobuf/GeneratedMessageV3
      "scala-compiler-",
      "scala-library-",
      "scala-parser-combinators_",
      "scala-reflect-",
      "scala-xml_",
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
  ShadeRule.rename("com.google.gson.**" -> "shade.@0").inAll//,
  //ShadeRule.rename("com.google.protobuf.**" -> "shade.@0").inAll
)


// Disable scaladoc generation

Compile / doc / sources := Seq.empty

//Compile / packageDoc / publishArtifact := false

Compile / packageBin / publishArtifact := true

Compile / packageSrc / publishArtifact := true

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

pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)

// Do not disable checksum
publishLocal / checksums := Nil

sonatypeCredentialHost := sonatypeCentralHost

ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

// Release
releaseCrossBuild := true

releaseIgnoreUntrackedFiles := true

releaseProcess := Seq(
//  checkSnapshotDependencies, //allow snapshot dependencies
  inquireVersions,
  runClean,
//  releaseStepCommand("+test"),
  setReleaseVersion,
  commitReleaseVersion, // forces to push dirty files
  tagRelease,
  releaseStepCommandAndRemaining("publishSigned"),
  releaseStepCommand("sonaRelease"),
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
    id = "tiboun",
    name = "Bounkong Khamphousone",
    email = "bounkong@gmail.com",
    url = url("https://github.com/tiboun")
  ),
  Developer(
    id = "fupelaqu",
    name = "Stéphane Manciot",
    email = "stephane.manciot@gmail.com",
    url = url("https://github.com/fupelaqu")
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

val packageSetup = Def.taskKey[Unit]("Package Setup.class")
packageSetup := {
  import java.nio.file.Paths
  def zipFile(from: List[java.nio.file.Path], to: java.nio.file.Path): Unit = {
    import java.util.jar.Manifest
    val manifest = new Manifest()
    manifest.getMainAttributes().putValue("Manifest-Version", "1.0")
    manifest.getMainAttributes().putValue("Implementation-Version", version.value)
    manifest.getMainAttributes().putValue("Implementation-Vendor", "starlake")
    manifest.getMainAttributes().putValue("Implementation-Vendor", "starlake")
    manifest.getMainAttributes().putValue("Compiler-Version", javacCompilerVersion)

    IO.jar(from.map(f => f.toFile -> f.toFile.getName()), to.toFile, manifest)

  }
  val scalaMajorVersion = scalaVersion.value.split('.').take(2).mkString(".")
  val setupClass = Paths.get(s"target/scala-$scalaMajorVersion/classes/Setup.class")
  val setupAuthenticatorClass = Paths.get(s"target/scala-$scalaMajorVersion/classes/Setup$$UserPwdAuth.class")
  val setupJarDependencyClass = Paths.get(s"target/scala-$scalaMajorVersion/classes/Setup$$ResourceDependency.class")
  val to = Paths.get("distrib/setup.jar")
  zipFile(
    List(setupClass, setupAuthenticatorClass, setupJarDependencyClass),
    to
  )
}

Compile / mainClass := Some("ai.starlake.job.Main")

// Compile / packageBin := ((Compile / packageBin).dependsOn(packageSetup)).value


Test / parallelExecution := false

// We want each test to run using its own spark context
Test / testGrouping :=  (Test / definedTests).value.map { suite =>
  Group(suite.name, Seq(suite), SubProcess(ForkOptions().withRunJVMOptions(testJavaOptions.toVector)))
}


