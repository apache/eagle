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

if [ $# -lt 1 ];
then
	echo "USAGE: $0 [-daemon] conf/zookeeper-server.properties"
	exit 1
fi
base_dir=$(dirname $0)

if [ "x$EAGLE_LOG4J_OPTS" = "x" ]; then
    export EAGLE_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../conf/log4j.properties"
fi

if [ "x$EAGLE_HEAP_OPTS" = "x" ]; then
    export EAGLE_HEAP_OPTS="-Xmx512M -Xms512M"
fi

EXTRA_ARGS="-name zookeeper -loggc"

COMMAND=$1
case $COMMAND in
  -daemon)
     EXTRA_ARGS="-daemon "$EXTRA_ARGS
     shift
     ;;
 *)
     ;;
esac

$base_dir/zookeeper-server-status.sh 1>/dev/null 2>&1
if [ "$?" == "" ];then
	echo "Zookeeper is still running, please stop firstly before starting"
	exit 0
else
	exec $base_dir/eagle-run-class.sh $EXTRA_ARGS org.apache.zookeeper.server.quorum.QuorumPeerMain "$@"
fi