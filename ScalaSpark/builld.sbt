ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "finalproject.ScalaSpark"
  )


scalaVersion := "2.13.6"

val sparkVersion = "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "de.halcony" %% "scala-argparse" % "1.1.11"
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.3"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.30.0"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.11" % "test"
