import Dependencies._

name := "comet"

version := "0.1"


lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

crossScalaVersions := supportedScalaVersions

organization := "com.ebiznext"

organizationName := "Ebiznext"

scalaVersion := scala211

organizationHomepage := Some(url("http://www.ebiznext.com"))

libraryDependencies := dependencies

Common.enableCometAliases

enablePlugins(Common.cometPlugins :_ *)

Common.customSettings


