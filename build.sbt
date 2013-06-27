name := "Scabaloo"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.1"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.3"

// For test
libraryDependencies += "com.h2database" % "h2" % "1.3.167" % "test"

libraryDependencies += "org.specs2" %% "specs2" % "1.14" % "test"

libraryDependencies += "org.mockito" % "mockito-all" % "1.9.0" % "test"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.10" % "test"
