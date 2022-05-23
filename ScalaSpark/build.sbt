import sbt.io.Using

ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "finalproject.ScalaSpark"
  )


scalaVersion := "2.13.6"

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "de.halcony" %% "scala-argparse" % "1.1.11",
  "org.postgresql" % "postgresql" % "42.3.3",
  "com.github.nscala-time" %% "nscala-time" % "2.30.0",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.11" % "test",
  "org.scala-lang" % "scala-library" % "2.13.6",
  "com.github.nscala-time" %% "nscala-time" % "2.30.0"
)

Compile / packageBin / packageOptions += {
  val file = new java.io.File("META-INF/MANIFEST.MF")
  val manifest = Using.fileInputStream(file)(in => new java.util.jar.Manifest(in))
  Package.JarManifest(manifest)
}
// spark-submit --packages de.halcony:scala-argparse_2.13:1.1.11 preprocessing.jar purchases local[*] purchases -m dev -datafp purchases.json -dataf json -rfp result