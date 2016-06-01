#!/bin/bash

cd $(dirname $0)/../../alert-assembly/

java -cp ${MAVEN_REPO}/org/apache/storm/storm-core/0.9.3/storm-core-0.9.3.jar:target/alert-engine-0.0.1-SNAPSHOT-alert-assembly.jar org.apache.eagle.alert.engine.e2e.SampleClient2

