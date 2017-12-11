import sbt.Keys.{libraryDependencies, version}


lazy val root = (project in file(".")).
  settings(
    name := "GeoSparkScalaTemplate",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization := "org.datasyslab",

    publishMavenStyle := true
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.2" % "compile",
  "org.datasyslab" % "geospark" % "1.0.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("org.datasyslab", "geospark", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"