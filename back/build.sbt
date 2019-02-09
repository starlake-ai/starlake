

name := "comet"

version := "0.1"

scalaVersion := "2.11.9"


scalacOptions += "-Xmacro-settings:materialize-derivations"




addCommandAlias("cd", "project") // navigate the projects

addCommandAlias("cc", ";clean;compile") // clean and compile

addCommandAlias("pl", ";clean;publishLocal") // clean and publish locally

addCommandAlias("pr", ";clean;publish") // clean and publish globally

addCommandAlias("pld", ";clean;local:publishLocal;dockerComposeUp") // clean and publish/launch the docker environment

// Libraries

val scalaTest = Seq("org.scalatest" %% "scalatest" % Versions.scalatest % "test")

val betterfiles = Seq("com.github.pathikrit" %% "better-files" % Versions.betterFiles)

val spark = Seq(
  "org.apache.spark" %% "spark-core" % Versions.spark % "provided",
  "org.apache.spark" %% "spark-sql" % Versions.spark % "provided",
  "org.apache.spark" %% "spark-hive" % Versions.spark % "provided",
  "org.apache.spark" %% "spark-mllib" % Versions.spark % "provided"
)

val logging = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
)


val typedConfigs = Seq("com.github.kxbmap" %% "configs" % Versions.configs)

val okhttp = Seq("com.squareup.okhttp3" % "okhttp" % Versions.okhttp)

val jackson = Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
  "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson,
  "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions.jackson
)

libraryDependencies ++= scalaTest ++ logging ++ typedConfigs ++ spark ++ okhttp ++ betterfiles ++ jackson

// Required when running tests in intellij
dependencyOverrides += "org.scalatest" %% "scalatest" % Versions.scalatest

// Assembly
mainClass in Compile := Some("com.ebiznext.comet.job.Main")

test in assembly := {}


// Git
enablePlugins(GitVersioning)


git.useGitDescribe := true

git.gitTagToVersionNumber := { tag: String =>
  if (tag matches "[0-9]+\\..*") Some(tag)
  else None
}

// Shinx plugin
enablePlugins(SphinxPlugin)

sourceDirectory in Sphinx := baseDirectory.value / ".." / "docs"

// Scaladoc
enablePlugins(SiteScaladocPlugin)

publishSite


// Format on compile
scalafmtOnCompile  := true,
