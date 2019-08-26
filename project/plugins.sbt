logLevel := Level.Warn

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.6.20")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.10")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.2")

addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.5.1")
