ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "bank-transaction-spark-streaming",
    version := "1.0",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3"
    ),

    Compile / run / fork := true,

    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
