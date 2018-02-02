#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions

function error() {
SCRIPT="$0"           # script name
LASTLINE="$1"         # line of error occurrence
LASTERR="$2"          # error code
echo "ERROR exit from ${SCRIPT} : line ${LASTLINE} with exit code ${LASTERR}"
exit 1
}

trap 'error ${LINENO} ${?}' ERR

echo ""
echo "Welcome to try Eagle"
echo ""

echo "Eagle home folder path is $EAGLE_HOME"
cd $EAGLE_HOME

# hbase tables will be autocreated now
#Initializing Eagle Service ...
#sh ./bin/eagle-service-init.sh
#
#sleep 10

#Starting Eagle Service ...
sh ./bin/eagle-server.sh start

sleep 10

echo "Creating kafka topics for eagle ... "
KAFKA_HOME=/usr/hdp/current/kafka-broker
EAGLE_ZOOKEEPER_QUORUM=$EAGLE_SERVER_HOST:2181
KAFKA_TOPIC_NAME=sandbox_hdfs_audit_log
topic=`${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper $EAGLE_ZOOKEEPER_QUORUM --topic $KAFKA_TOPIC_NAME`
if [ -z $topic ]; then
        $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $EAGLE_ZOOKEEPER_QUORUM --replication-factor 1 --partitions 1 --topic $KAFKA_TOPIC_NAME
        if [ $? = 0 ]; then
            echo "==> Created kafka topic: $KAFKA_TOPIC_NAME successfully for eagle"
        else
            echo "==> Failed to create required topic: $KAFKA_TOPIC_NAME, exiting"
            exit 1
        fi
else
    echo "==> Kafka topic:$KAFKA_TOPIC_NAME already exists for eagl"
fi

EAGLE_NIMBUS_HOST=$EAGLE_SERVER_HOST
EAGLE_SERVICE_HOST=$EAGLE_SERVER_HOST
EAGLE_TOPOLOGY_JAR=`ls ${EAGLE_HOME}/lib/eagle-topology-*-assembly.jar`

# TODO: More eagle start

echo "Eagle is deployed successfully!"
echo "Please visit http://$EAGLE_SERVER_HOST:9090 to play with Eagle!"
