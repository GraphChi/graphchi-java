organization := "org.graphchi"

name := "graphchi-java"

version := "0.2.2"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.11.2", "2.10.3")

javaSource in Test := baseDirectory.value / "test"

libraryDependencies ++= Seq(
  "com.yammer.metrics" % "metrics-core" % "2.2.0",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.apache.pig" % "pig" % "0.10.0",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2",
  "org.apache.commons" % "commons-math" % "2.1",
  "commons-cli" % "commons-cli" % "1.2",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.4" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

publishMavenStyle := true

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

licenses := Seq("Apache-2.0" -> url("http://www.opensource.org/licenses/Apache-2.0"))

homepage := Some(url("http://github.com/GraphChi/graphchi-java"))

pomExtra := (
  <scm>
    <url>git@github.com:GraphChi/graphchi-java.git</url>
    <connection>scm:git:git@github.com:GraphChi/graphchi-java.git</connection>
  </scm>
  <developers>
    <developer>
      <id>matt-gardner</id>
      <name>Matt Gardner</name>
      <url>http://cs.cmu.edu/~mg1</url>
    </developer>
  </developers>)
