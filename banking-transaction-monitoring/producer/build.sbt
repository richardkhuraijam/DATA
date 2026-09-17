ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "bank-transaction-producer",
    version := "1.0"
  )

libraryDependencies +=
  "org.apache.kafka" % "kafka-clients" % "3.7.0"
