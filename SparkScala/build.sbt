ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.6"

lazy val root = (project in file("."))
  .settings(
    name := "CapstoneProject"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0"
libraryDependencies += "de.halcony" %% "scala-argparse" % "1.1.10"

