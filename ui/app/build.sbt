lazy val akkaHttpVersion = "10.0.11"
lazy val akkaVersion = "2.5.11"
lazy val rocksdbVersion = "5.7.3"
lazy val json4sJackson = "3.5.4"
lazy val commonsIOVersion = "2.6"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.ebiznext",
      scalaVersion := "2.12.4"
    )),
    name := "comet-api",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "org.json4s" %% "json4s-jackson" % json4sJackson,
      "org.rocksdb" % "rocksdbjni" % rocksdbVersion,
      "commons-io" % "commons-io" % commonsIOVersion,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test
    )
  )
