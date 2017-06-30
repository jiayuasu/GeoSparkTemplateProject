#!/bin/bash
cd ./geospark/scala
sbt assembly
cd ../java
mvn clean install
cd ../../babylon/scala
sbt assembly
cd ../java
mvn clean install
