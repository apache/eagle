#!/bin/bash

echo $(dirname $0)

#start topology
echo "starting topology..."
cd $(dirname $0)/../../alert-engine/alert-engine-base/


echo " as dev tests, tail -f test.log | grep AlertStreamEvent for alert stream....."

mvn test -Dtest=org.apache.eagle.alert.engine.e2e.Integration1 | tee test.log

# tail log output
# tail -f test.log | grep AlertStreamEvent

