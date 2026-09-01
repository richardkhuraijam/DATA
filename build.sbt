ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-log-analysis",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.6",
      "org.apache.spark" %% "spark-sql" % "3.5.6"
    ),

    Compile / run / fork := true,

    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
