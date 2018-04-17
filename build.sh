#!/bin/bash
cd ./geospark/scala
sbt clean assembly
java -cp ./target/scala-2.11/GeoSparkScalaTemplate-assembly-0.1.0.jar ScalaExample
cd ../java
mvn clean install
java -cp ./target/GeoSparkJavaTemplate-0.1.0.jar example.Example
cd ../../geospark-viz/scala
sbt clean assembly
java -cp ./target/scala-2.11/GeoSparkVizScalaTemplate-assembly-0.1.0.jar ScalaExample
cd ../java
mvn clean install
java -cp ./target/GeoSparkVizJavaTemplate-0.1.0.j example.Example
cd ../../geospark-sql/scala
sbt clean assembly
java -cp ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar ScalaExample
cd ../../geospark-analysis
sbt clean assembly
java -cp ./target/scala-2.11/geospark-analysis-assembly-0.1.0.jar ScalaExample