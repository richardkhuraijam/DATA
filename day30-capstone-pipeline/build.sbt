ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"
ThisBuild / organization := "learning.scala.spark"

lazy val root = (project in file("."))
  .settings(
    name := "day30-capstone-pipeline",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3",
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
