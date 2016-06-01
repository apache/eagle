#!/bin/bash

echo $(dirname $0)

export MAVEN_OPTS=-Xms256M -Xmx1024M

#start topology
echo "starting topology..."
cd $(dirname $0)/../../alert-engine/alert-engine-base/


echo " as dev tests, tail -f test.log | grep AlertStreamEvent for alert stream....."


mvn test -Dtest=org.apache.eagle.alert.engine.e2e.Integration2 | tee test.log

# tail log output
# tail -f test.log | grep AlertStreamEvent

