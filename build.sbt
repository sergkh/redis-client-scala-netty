name := "redis-scala"

organization := "com.impactua"

version := sys.env.getOrElse("TRAVIS_TAG", "1.4.0-SNAPSHOT")

scalaVersion := "2.12.11"

crossScalaVersions := Seq("2.11.8", "2.12.11", "2.13.1")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

bintrayReleaseOnPublish := false
bintrayPackage := name.value
bintrayOrganization in bintray := Some("sergkh")
bintrayPackageLabels := Seq("scala", "redis", "netty")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

val nettyVersion = "4.1.48.Final"

libraryDependencies ++= Seq(
  "io.netty"           % "netty-handler"                 % nettyVersion,
  "io.netty"           % "netty-transport-native-epoll"  % nettyVersion classifier "linux-x86_64",
  "io.netty"           % "netty-transport-native-kqueue" % nettyVersion classifier "osx-x86_64",
  "org.scalatest"     %% "scalatest"                     % "3.1.1" % Test,
  "com.storm-enroute" %% "scalameter"                    % "0.19" % Test
)
