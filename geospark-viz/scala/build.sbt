import sbt.Keys.{libraryDependencies, version}



lazy val root = (project in file(".")).
  settings(
    name := "GeoSparkVizScalaTemplate",

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
  "org.apache.spark" %% "spark-core" % "2.1.1" % "compile",
  "org.datasyslab" % "geospark" % "1.0.0",
  "org.datasyslab" % "geospark-viz" % "1.0.0",
  "org.datasyslab" % "sernetcdf" % "0.1.0"
)

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"