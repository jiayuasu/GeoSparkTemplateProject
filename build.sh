#!/bin/bash
cd ./geospark/scala
sbt assembly
cd ../java
mvn clean install
cd ../../geospark-viz/scala
sbt assembly
cd ../java
mvn clean install
cd ../../geospark-sql/scala
sbt assembly