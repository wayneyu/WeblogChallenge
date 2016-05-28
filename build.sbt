name := """weblogchallenge"""

version := "1.0"

scalaVersion := "2.10.6"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.6" % "test"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.4"

libraryDependencies += "joda-time" % "joda-time" % "2.9.3"

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

