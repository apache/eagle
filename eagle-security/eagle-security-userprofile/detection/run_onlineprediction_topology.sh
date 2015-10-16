#!/usr/bin/env bash
export JAVA_HOME=$(/usr/libexec/java_home -v 1.6)
mvn -X exec:java -Dexec.mainClass="eagle.security.userprofile.UserProfileDetectionAnomalyRunner"
