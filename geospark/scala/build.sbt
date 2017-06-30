import sbt.Keys.{libraryDependencies, version}



lazy val root = (project in file(".")).
  settings(
    name := "GeoSparkScalaTemplate",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization  := "org.datasyslab",

    publishMavenStyle := true
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.datasyslab" % "geospark" % "0.8.0"
)