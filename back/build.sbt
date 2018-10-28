import Common._

name := "comet"

version := "0.1"

scalaVersion in ThisBuild := "2.11.12"


scalacOptions += "-Xmacro-settings:materialize-derivations"


addCommandAlias("cd", "project") // navigate the projects

addCommandAlias("cc", ";clean;compile") // clean and compile

addCommandAlias("pl", ";clean;publishLocal") // clean and publish locally

addCommandAlias("pr", ";clean;publish") // clean and publish globally

addCommandAlias("pld", ";clean;local:publishLocal;dockerComposeUp") // clean and publish/launch the docker environment

val json4sNative = "org.json4s" %% "json4s-native" % Versions.json4s

val json4sJackson = "org.json4s" %% "json4s-jackson" % Versions.json4s

val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalatest % "test"


val log4JExclusions = Seq(
  ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j"),
  ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"),
  ExclusionRule(organization = "org.slf4j", name = "log4j-over-slf4j"),
  ExclusionRule(organization = "log4j", name = "log4j")
)

val webServersExclusions = Seq(
  ExclusionRule(organization = "org.mortbay.jetty"),
  ExclusionRule(organization = "com.sun.jersey"),
  ExclusionRule(organization = "org.eclipse.jetty", name = "jetty-server"),
  ExclusionRule(organization = "com.sun.jersey.contribs", name = "jersey-guice"),
  ExclusionRule(organization = "javax.servlet", name = "servlet-api"),
  ExclusionRule(organization = "javax.servlet.jsp", name = "jsp-api")
)


val logging = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging,
  "ch.qos.logback" % "logback-classic" % Versions.logback,
  "org.slf4j" % "log4j-over-slf4j" % Versions.slf4j
)


val hadoop = Seq(
  "org.apache.hadoop" % "hadoop-common" % Versions.hadoop,
  "org.apache.hadoop" % "hadoop-client" % Versions.hadoop,
  "org.apache.hadoop" % "hadoop-hdfs" % Versions.hadoop
).map(_.excludeAll(log4JExclusions ++ webServersExclusions: _*))


val pureConfig = Seq(
  "com.typesafe" % "config" % Versions.typesafeConfig,
  "com.github.pureconfig" %% "pureconfig" % Versions.pureConfig
)

val logbackExclusions = Seq(
  ExclusionRule(organization = "ch.qos.logback")
)

val sparkExclusions = Seq(
  ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-client"),
  ExclusionRule(organization = "org.apache.zookeeper", name = "zookeeper"),
  ExclusionRule(organization = "org.apache.curator", name = "curator-recipes"),
  ExclusionRule(organization = "org.apache.curator", name = "curator-client"),
  ExclusionRule(organization = "org.apache.curator", name = "curator-framework"),
  ExclusionRule(organization = "net.java.dev.jets3t", name = "jets3t")
)

val allSparkExclusions
: Seq[ExclusionRule] = logbackExclusions ++ log4JExclusions ++ webServersExclusions ++ sparkExclusions

val spark = Seq(
  "org.apache.spark" %% "spark-core" % Versions.spark % "provided,test",
  "org.apache.spark" %% "spark-sql" % Versions.spark % "provided,test",
  "org.apache.spark" %% "spark-mesos" % Versions.spark % "provided,test"
).map(_.excludeAll(allSparkExclusions: _*)) ++
  logging ++
  Seq(
    "org.apache.hadoop" % "hadoop-client" % Versions.hadoop % "provided,test",
    "org.apache.zookeeper" % "zookeeper" % Versions.zookeeper % "provided,test"
  ).map(_.excludeAll(allSparkExclusions: _*))



enablePlugins(JavaAppPackaging)


libraryDependencies in ThisBuild ++= Seq(json4sNative, json4sJackson, scalaTest) ++ hadoop  ++ logging ++ pureConfig ++ spark

val corePath = file("src") / "core"

lazy val schema = library("schema", corePath / "schema")


