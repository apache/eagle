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

export EAGLE_BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
export EAGLE_BUILD_DIR=${EAGLE_BASE_DIR}/eagle-assembly/target/eagle-*-bin/eagle-*/
chmod +x $EAGLE_BASE_DIR/eagle-assembly/src/main/bin/*

echo "Stopping eagle service"
ls ${EAGLE_BUILD_DIR} 1>/dev/null 2>/dev/null
if [ "$?" != "0" ];then
	echo "$EAGLE_BUILD_DIR not exist, do nothing"
else
	$EAGLE_BUILD_DIR/bin/eagle-service.sh stop
fi
echo "Stopping zookeeper"
$EAGLE_BASE_DIR/eagle-assembly/src/main/bin/kafka-server-stop.sh
echo "Stopping kafka"
$EAGLE_BASE_DIR/eagle-assembly/src/main/bin/zookeeper-server-stop.sh