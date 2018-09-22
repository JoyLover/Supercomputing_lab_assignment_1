import sbt.Keys.libraryDependencies

ThisBuild / scalaVersion := "2.11.12"

lazy val lab = (project in file("."))
  .settings(
    name := "lab_1_assignment",
    fork in run := true,

    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1",
    libraryDependencies += "io.spray" %%  "spray-json" % "1.3.4",
    libraryDependencies += "net.liftweb" %% "lift-json" % "2.6",
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.3.0"
  )
