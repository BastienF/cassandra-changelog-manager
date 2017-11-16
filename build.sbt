name := "cassandra-changelog-manager"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",
  "com.typesafe" % "config" % "1.3.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.scalatest" % "scalatest_2.12" % "3.0.4" % "test",
  "org.mockito" % "mockito-core" % "2.12.0" % "test"

)

parallelExecution in Test := false

mainClass in assembly := Some("cassandra.changelog_manager.Main")

assemblyMergeStrategy in assembly := {
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
