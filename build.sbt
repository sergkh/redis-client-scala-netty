name := "redis-scala"

organization := "com.impactua"

version := sys.env.getOrElse("TRAVIS_TAG", "1.4.0-SNAPSHOT")

scalaVersion := "2.12.4"

crossScalaVersions := Seq("2.11.8", "2.12.0")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

bintrayReleaseOnPublish := false
bintrayPackage := name.value
bintrayOrganization in bintray := Some("yarosman")
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

val nettyVersion = "4.1.20.Final"

libraryDependencies ++= Seq(
  "io.netty"           % "netty-handler"                % nettyVersion,
  "io.netty"           % "netty-transport-native-epoll" % nettyVersion classifier "linux-x86_64",
  "org.scalatest"     %% "scalatest"                    % "3.0.4" % Test,
  "com.storm-enroute" %% "scalameter"                   % "0.8.2" % Test
)
