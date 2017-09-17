import sbt.Keys.{libraryDependencies, version}



lazy val root = (project in file(".")).
  settings(
    name := "BabylonScalaTemplate",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization  := "org.datasyslab",

    publishMavenStyle := true
  )

assemblyMergeStrategy in assembly := {
  case PathList("org.datasyslab", "geospark", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.datasyslab" % "geospark" % "0.8.2",
  "org.datasyslab" % "babylon" % "0.2.2"
)
