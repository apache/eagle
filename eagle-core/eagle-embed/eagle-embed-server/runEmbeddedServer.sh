#!/bin/bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
mvn -X exec:java -Dexec.mainClass="org.apache.eagle.service.embedded.tomcat.EmbeddedServer" -Dexec.args="../../../eagle-security/eagle-security-webservice/target/eagle-service 38080"