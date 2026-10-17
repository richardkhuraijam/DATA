ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day24-spark-streaming-fraud-detection",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3"
    ),

    Compile / run / fork := true,

    Compile / run / javaOptions ++= Seq(
      "-Xms512m",
      "-Xmx2g",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
  )
