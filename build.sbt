import Dependencies._

name := "comet"

version := "0.1"

val mavenLocal = "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

//resolvers += Resolver.mavenLocal


lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

crossScalaVersions := supportedScalaVersions

organization := "com.ebiznext"

organizationName := "Ebiznext"

scalaVersion := scala211

organizationHomepage := Some(url("http://www.ebiznext.com"))

libraryDependencies := {
  val spark = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, scalaMajor)) if scalaMajor == 12 => spark212
      case Some((2, scalaMajor)) if scalaMajor == 11 => spark211_240
    }
  }
  dependencies ++ spark
}

Common.enableCometAliases

enablePlugins(Common.cometPlugins :_ *)

Common.customSettings


