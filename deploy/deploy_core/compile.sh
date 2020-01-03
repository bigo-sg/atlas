#!/usr/bin/env bash
echo "compiling project bigo-atlas 2.0"
cd ../../
mvn clean package \
-DskipTests \
-Pdist
-Drat.numUnapprovedLicenses=100