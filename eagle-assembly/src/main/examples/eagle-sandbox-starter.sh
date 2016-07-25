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

# This script will start Eagle service with storage Derby,
# and a sample application HiveQueryLog monitoring

source $(dirname $0)/../bin/eagle-env.sh
eagle_bin=$EAGLE_HOME/bin


###################################################################
#             STEP 1: Check Installation
###################################################################

echo "STEP [1/3]: checking environment"
$eagle_bin/eagle-check-env.sh
[ $? != 0 ] && exit 1

pid_dir=/var/run

# Check storm if it has been started
nimbus_pid=$pid_dir/storm/nimbus.pid
supervisor_pid=$pid_dir/storm/supervisor.pid
ui_pid=$pid_dir/storm/ui.pid
echo "Checking if storm is running ..."

if ! ([ -f $nimbus_pid ] && ps aux | grep -v grep | grep $(cat $nimbus_pid) > /dev/null)
then
    echo "Error: Storm Nimbus is not running"
    exit 1
fi

if ! ([ -f $supervisor_pid ] && ps aux | grep -v grep | grep $(cat $supervisor_pid) > /dev/null)
then
    echo "Error: Storm Supervisor is not running"
    exit 1
fi

if ! ([ -f $ui_pid ] && ps aux | grep -v grep | grep $(cat $ui_pid) > /dev/null)
then
    echo "Error: Storm UI is not running"
    exit 1
fi

echo "Storm is running"


###################################################################
#              STEP 2: Starting Eagle Service
###################################################################

echo "STEP [2/3]: start eagle service"
$eagle_bin/eagle-service.sh start


###################################################################
#              STEP 3: Starting Eagle Topology
###################################################################

echo "STEP [3/3]: start eagle topology"

$eagle_bin/eagle-topology-init.sh
[ $? != 0 ] && exit 1
${EAGLE_HOME}/examples/sample-sensitivity-resource-create.sh
[ $? != 0 ] && exit 1
${EAGLE_HOME}/examples/sample-policy-create.sh
[ $? != 0 ] && exit 1

$eagle_bin/eagle-topology.sh --main org.apache.eagle.security.hive.jobrunning.HiveJobRunningMonitoringMain --config ${EAGLE_HOME}/conf/sandbox-hiveQueryLog-application.conf start


