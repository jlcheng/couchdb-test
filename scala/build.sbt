name := "Scala CouchDB Test"

version := "1.0"

organization := "jchengt"

scalaVersion := "2.9.0"

scalacOptions ++= Seq("-unchecked", "-deprecation")

// Add multiple dependencies
libraryDependencies ++= Seq(
    "junit" % "junit" % "4.8" % "test",
    "com.google.guava" % "guava" % "r05"
)

mainClass in (Compile, run) := Some("jcheng.CouchDBTest")




