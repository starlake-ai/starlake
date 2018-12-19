

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
  "org.apache.spark" %% "spark-hive" % Versions.spark % "provided"
)

val logging = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
)

//val pureConfig = Seq(
//  "com.typesafe" % "config" % Versions.typesafeConfig,
//  "com.github.pureconfig" %% "pureconfig" % Versions.pureConfig
//).map(_.exclude("com.chuusai", "shapeless_2.11"))

val pureConfig = Seq("com.typesafe" % "config" % Versions.typesafeConfig)


//val shapeless = Seq("com.chuusai" %% "shapeless" % "2.0.0" % "provided")

val postgres = Seq("org.postgresql" % "postgresql" % "42.2.5")

val hikaricp = Seq("com.zaxxer" % "HikariCP" % "2.5.1")

val okhttp = Seq("com.squareup.okhttp3" % "okhttp" % "3.11.0")

val jackson = Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
  "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson,
  "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions.jackson
)

libraryDependencies ++= scalaTest ++
  logging ++ pureConfig ++ postgres ++ hikaricp ++ spark ++
  /* shapeless ++ */ okhttp ++ betterfiles ++ jackson


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