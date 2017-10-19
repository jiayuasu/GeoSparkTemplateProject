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
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.datasyslab" % "geospark" % "0.9.0-SNAPSHOT",
  "org.geotools" % "gt-metadata" % "17.0" exclude("com.vividsolutions", "jts"),
  "org.geotools" % "gt-data" % "17.0" exclude("com.vividsolutions", "jts")
)


resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"

resolvers +=
  "Java.net repository" at "http://download.java.net/maven/2"