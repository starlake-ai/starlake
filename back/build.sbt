import Common._

name := "comet"

version := "0.1"

scalaVersion := "2.11.12"


scalacOptions += "-Xmacro-settings:materialize-derivations"


addCommandAlias("cd", "project") // navigate the projects

addCommandAlias("cc", ";clean;compile") // clean and compile

addCommandAlias("pl", ";clean;publishLocal") // clean and publish locally

addCommandAlias("pr", ";clean;publish") // clean and publish globally

addCommandAlias("pld", ";clean;local:publishLocal;dockerComposeUp") // clean and publish/launch the docker environment

val json4sNative = "org.json4s" %% "json4s-native" % Versions.json4s

val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalatest % "test"

val logging = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
)

val pureConfig = Seq(
  "com.typesafe" % "config" % Versions.typesafeConfig,
  "com.github.pureconfig" %% "pureconfig" % Versions.pureConfig
)

val postgres = Seq( "org.postgresql" % "postgresql" % "42.2.5")

//.map(_.excludeAll(allSparkExclusions: _*))


//excludeDependencies  ++= Seq("org.slf4j" % "slf4j-log4j12", "log4j" %"log4j")

val hikaricp = Seq("com.zaxxer" % "HikariCP" % "2.5.1")

libraryDependencies ++= Seq(json4sNative, scalaTest) ++ logging  ++ pureConfig ++ postgres ++ hikaricp // ++ spark

Compile / unmanagedJars ++= {
  val base = file("/Users/hayssams/programs/spark-2.3.2-bin-hadoop2.7/jars")
  val customJars = base ** "*.jar"
  customJars.classpath
}
