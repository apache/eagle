#!/bin/bash

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

export EAGLE_BASE_DIR=$(dirname $0)/../../../
export EAGLE_ASSEMBLY_TARGET=${EAGLE_BASE_DIR}/eagle-assembly/target/eagle-*-bin/eagle-*/

ls ${EAGLE_ASSEMBLY_TARGET} 1>/dev/null 2>/dev/null
if [ "$?" != "0" ];then
	echo "$EAGLE_ASSEMBLY_TARGET not exist, build now"
	cd $EAGLE_BASE_DIR
	mvn package -DskipTests
fi

cd $EAGLE_ASSEMBLY_TARGET/

bin/eagle-service.sh status

if [ "$?" != "0" ];then
	echo "Starting eagle service ..."
	bin/eagle-service.sh start
	echo "Wait 5 seconds for eagle service to be ready .. "
	sleep 5
else
	echo "Eagle service has already started"
fi

bin/eagle-topology-init.sh

cd $(dirname $0)/../

chmod +x bin/*.sh

bin/init.sh

$EAGLE_ASSEMBLY_TARGET/bin/kafka-stream-monitor.sh cassandraQueryLogStream cassandraQueryLogExecutor $(dirname $0)/../conf/cassandra-security-local.conf