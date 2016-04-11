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

source $(dirname $0)/eagle-env.sh

if [ -e ${EAGLE_HOME}/lib/topology/eagle-topology-*-assembly.jar ];then
	export jar_name=$(ls ${EAGLE_HOME}/lib/topology/eagle-topology-*-assembly.jar)
else
	echo "ERROR: ${EAGLE_HOME}/lib/topology/eagle-topology-*-assembly.jar not found"
	exit 1
fi

export main_class=$1

if [ "$#" -lt "1" ];then
	echo "ERROR: parameter required"
	echo "Usage: $0 [main-class]"
	echo ""
	exit 1
fi

which storm >/dev/null 2>&1
if [ $? != 0 ];then
    echo "ERROR: storm not found"
    exit 1
else
	export EAGLE_CLASSPATH=$EAGLE_CLASSPATH:$jar_name:`storm classpath`
fi

cmd="java -cp $EAGLE_CLASSPATH $main_class "

echo "Run Cmd: $cmd"
echo $cmd
$cmd