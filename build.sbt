import Dependencies.*
import sbt.Tests.{Group, SubProcess}
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations.*
import sbtrelease.Version.Bump.Next
import xerial.sbt.Sonatype.*

lazy val javacCompilerVersion = "11"

javacOptions ++= Seq(
  "-source", javacCompilerVersion,
  "-target", javacCompilerVersion,
  "-Xlint"
)

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

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"

lazy val scala212 = "2.12.20"

lazy val scala213 = "2.13.15"

lazy val supportedScalaVersions = List(scala212, scala213)

 ThisBuild / crossScalaVersions := supportedScalaVersions

organization := "ai.starlake"

organizationName := "starlake"

ThisBuild / scalaVersion := scala213 // scala213 scala212

organizationHomepage := Some(url("https://github.com/starlake-ai/starlake"))

resolvers ++= Resolvers.allResolvers

libraryDependencies ++= {
  val versionSpecificLibs = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => scala212LibsOnly
      case Some((2, 13)) => scalaCompat ++ scala213LibsOnly
      case _ => throw new Exception(s"Invalid Scala Version")
    }
  }
  dependencies ++ spark_3d0_forScala_2d12 ++
    jackson212ForSpark3 ++ esSpark212 ++
    pureConfig212 ++ scalaReflection(scalaVersion.value) ++
    versionSpecificLibs
}

dependencyOverrides := Seq(
 "com.google.protobuf"                % "protobuf-java"             % "3.25.5",
  "org.scala-lang"                    % "scala-library"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-reflect"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-compiler"            % scalaVersion.value,
  "com.google.guava"                  %  "guava"                    % "31.1-jre", // required by jinjava 2.7.3
  "com.fasterxml.jackson.dataformat"  % "jackson-dataformat-csv"    % Versions.jackson212ForSpark3
)

name := {
  val sparkNameSuffix = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => "3"
      case Some((2, 13)) => "3"
      case _             => throw new Exception(s"Invalid Scala Version")
    }
  }
  s"starlake-core"
}

assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-assembly.jar"

Common.enableStarlakeAliases

enablePlugins(Common.starlakePlugins: _*)


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
    "-Ywarn-unused:imports"
  ) ++ extractOptions

}


Common.customSettings

// Builds a far JAR with embedded spark libraries and other provided libs.
// Can be useful for running YAML generation without having a spark distribution
commands += Command.command("assemblyWithSpark") { state =>
  """set assembly / fullClasspath := (Compile / fullClasspath).value""" :: "assembly" :: state
}


Compile / assembly / artifact := {
  val art: Artifact = (Compile / assembly / artifact).value
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
  ShadeRule.rename("com.google.gson.**" -> "shade.@0").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "shade.@0").inAll,
  ShadeRule.rename("pureconfig.**" -> "shadepureconfig.@0").inAll,
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inLibrary("com.chuusai" % "shapeless_2.12" % "2.3.3"),
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inLibrary("com.chuusai" % "shapeless_2.13" % "2.3.3"),
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inLibrary("com.github.pureconfig" % "pureconfig_2.12" % Versions.pureConfig212ForSpark3),
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inLibrary("com.github.pureconfig" % "pureconfig_2.13" % Versions.pureConfig212ForSpark3),
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inLibrary("com.github.pureconfig" % "pureconfig-generic_2.12" % Versions.pureConfig212ForSpark3),
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inLibrary("com.github.pureconfig" % "pureconfig-generic_2.13" % Versions.pureConfig212ForSpark3)
  .inProject
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
//  checkSnapshotDependencies, //allow snapshot dependencies
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

Compile / packageBin := ((Compile / packageBin).dependsOn(packageSetup)).value


Test / parallelExecution := false

// We want each test to run using its own spark context
Test / testGrouping :=  (Test / definedTests).value.map { suite =>
  Group(suite.name, Seq(suite), SubProcess(ForkOptions().withRunJVMOptions(testJavaOptions.toVector)))
}


