
enablePlugins(JavaServerAppPackaging)

lazy val akkaHttpVersion = "10.0.11"
lazy val akkaVersion = "2.5.11"
lazy val rocksdbVersion = "5.7.3"
lazy val commonsIOVersion = "2.6"
lazy val commonsLang3Version = "3.7"
lazy val typesafeConfig = "1.3.3"
lazy val configs = "0.4.4"
lazy val scalaLogging = "3.9.0"
lazy val logback = "1.2.3"
lazy val log4s = "1.3.6"
lazy val json4sVersion = "3.5.2"
lazy val akkaHttpJsonVersion = "1.16.0"


lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.ebiznext",
      scalaVersion := "2.12.4"
    )),
    resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      Resolver.bintrayRepo("hseeberger", "maven")),
    name := "comet-api",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "org.json4s"        %% "json4s-native"   % json4sVersion,
      "org.json4s"        %% "json4s-ext"      % json4sVersion,
      "org.json4s"        %% "json4s-jackson" % json4sVersion,
      "de.heikoseeberger" %% "akka-http-json4s" % akkaHttpJsonVersion,
      "com.typesafe.akka" %% "akka-http-xml" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "org.rocksdb" % "rocksdbjni" % rocksdbVersion,
      "commons-io" % "commons-io" % commonsIOVersion,
      "com.typesafe"      % "config"   % typesafeConfig,
      "com.typesafe.scala-logging" %% "scala-logging"  % scalaLogging,
      "ch.qos.logback"             % "logback-classic" % logback,
      "org.log4s"                  %% "log4s"          % log4s,
      "com.github.kxbmap" %% "configs" % configs,
      "org.apache.commons" % "commons-lang3" % commonsLang3Version,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test
    )
  )
