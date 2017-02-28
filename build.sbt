name := "redis-scala"

organization := "com.impactua"

version := sys.env.getOrElse("TRAVIS_TAG", "1.4.0-SNAPSHOT")

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.4", "2.11.8", "2.12.0")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

bintrayReleaseOnPublish := false

bintrayPackage := name.value

bintrayOrganization in bintray := Some("sergkh")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.10.6.Final",
  "io.netty" % "netty-all" % "4.1.7.Final",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "com.storm-enroute" %% "scalameter" % "0.8.2" % Test
)
