import Dependencies._

name := "comet"

version := "0.1"

organization := "com.ebiznext"

organizationName := "Ebiznext"

scalaVersion := "2.11.9"

organizationHomepage := Some(url("http://www.ebiznext.com"))

libraryDependencies := dependencies

Common.enableCometAliases

enablePlugins(Common.cometPlugins :_ *)

Common.customSettings

publishSite
