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

source $(dirname $0)/../bin/eagle-env.sh
eagle_bin=$EAGLE_HOME/bin


###################################################################
#             STEP 1: Check Installation
###################################################################

echo "STEP [1/3]: checking environment"
$eagle_bin/eagle-check-env.sh
[ $? != 0 ] && exit 1

pid_dir=/var/run

# Check HBase if it has been started
hbase_master_pid=${pid_dir}/hbase/hbase-hbase-master.pid
hbase_regionserver_pid=${pid_dir}/hbase/hbase-hbase-regionserver.pid
echo "Checking if hbase is running ..."

if [ -f $hbase_master_pid ] && \
	ps aux | grep -v grep | grep $(cat $hbase_master_pid) > /dev/null
then
	echo "HBase Master is running as process `cat $hbase_master_pid`."
else
	echo "Error: HBase Master is not running. Please start it via Ambari."
	exit 1
fi

if [ -f $hbase_regionserver_pid ] && \
	ps aux | grep -v grep | grep $(cat $hbase_regionserver_pid) > /dev/null
then
	echo "HBase RegionServer is running as process `cat $hbase_regionserver_pid`."
else
	echo "Error: HBase RegionServer is not running. Please start it via Ambari."
	exit 1
fi

# Check kafka if it has been started
kafka_pid=$pid_dir/kafka/kafka.pid
echo "Checking if kafka is running ..."

if [ -f $kafka_pid ] && ps aux | grep -v grep | grep $(cat $kafka_pid) > /dev/null
then
	echo "Kafka is running as process `cat $kafka_pid`."
else
	echo "Error: Kafka is not running. Please start it via Ambari."
	exit 1
fi

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
$eagle_bin/eagle-service-init.sh
[ $? != 0 ] && exit 1

echo "Creating kafka topics for eagle ... "
KAFKA_HOME=/usr/hdp/current/kafka-broker
EAGLE_ZOOKEEPER_QUORUM=localhost:2181
topic=`${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper $EAGLE_ZOOKEEPER_QUORUM --topic sandbox_hdfs_audit_log`
if [ -z $topic ]; then
	$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $EAGLE_ZOOKEEPER_QUORUM --replication-factor 1 --partitions 1 --topic sandbox_hdfs_audit_log
fi

if [ $? = 0 ]; then
echo "==> Create kafka topic successfully for eagle"
else
echo "==> Failed, exiting"
exit 1
fi
$eagle_bin/eagle-topology-init.sh
[ $? != 0 ] && exit 1
${EAGLE_HOME}/examples/sample-sensitivity-resource-create.sh
[ $? != 0 ] && exit 1
${EAGLE_HOME}/examples/sample-policy-create.sh
[ $? != 0 ] && exit 1
$eagle_bin/eagle-topology.sh --main org.apache.eagle.security.auditlog.HdfsAuditLogProcessorMain --config ${EAGLE_HOME}/conf/sandbox-hdfsAuditLog-application.conf start
[ $? != 0 ] && exit 1
$eagle_bin/eagle-topology.sh --main org.apache.eagle.security.hive.jobrunning.HiveJobRunningMonitoringMain --config ${EAGLE_HOME}/conf/sandbox-hiveQueryLog-application.conf start
[ $? != 0 ] && exit 1
$eagle_bin/eagle-topology.sh --main org.apache.eagle.security.userprofile.UserProfileDetectionMain --config ${EAGLE_HOME}/conf/sandbox-userprofile-topology.conf start
[ $? != 0 ] && exit 1

