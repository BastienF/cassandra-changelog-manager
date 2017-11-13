name := "cassandra-changelog-manager"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",
  "com.typesafe" % "config" % "1.3.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.specs2" %% "specs2-core" % "4.0.0" % "test",
  "org.specs2" %% "specs2-mock" % "4.0.0" % "test"
)


mainClass in assembly := Some("Main")

assemblyMergeStrategy in assembly := {
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
