logLevel := Level.Warn

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.25")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1") 
     // version 0.9.2 is incompatible with sbt >= 1.3.x, see https://github.com/jrudolph/sbt-dependency-graph/issues/178

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")

// Had to rollback to sbt-git 2.0.0 because of a regression during release
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.3")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")

addSbtPlugin("com.lightbend" % "sbt-google-cloud-storage" % "0.0.10")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.20")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")

// provides "sbt dependencyUpdates":
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")
