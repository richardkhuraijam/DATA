name := "day5-spark-dataframes"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.3"
)

fork := true

javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)
