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

# Eagle Ambari Plugin
#
# Author: hchen9@xyz.com
# Version: 0.0.1
#
# Plan:
# 1. Implement create service through REST API: https://cwiki.apache.org/confluence/display/AMBARI/Adding+a+New+Service+to+an+Existing+Cluster
# 2. Integrate with ambari configuration

AMBARI_HOST=localhost
AMBARI_PORT=8080
AMBARI_CLUSTER_NAME=Sandbox
AMBARI_USER=admin
AMBARI_PASSWORD=admin

EAGLE_VERSION=0.0.1
HDP_VERSION=2.0.6

AMBARI_SERVER_HOME=/var/lib/ambari-server/
AMBARI_EAGLE_STACK_DIR=/var/lib/ambari-server/resources/stacks/HDP/${HDP_VERSION}/services/EAGLE/
AMBARI_EAGLE_COMMON_DIR=/var/lib/ambari-server/resources/common-services/EAGLE/${EAGLE_VERSION}/
DEFAULT_EAGLE_HOME=/usr/hdp/current/eagle
AMBARI_SERVER_APP_JS=/usr/lib/ambari-server/web/javascripts/app.js.gz

cd `dirname $0`/../

function usage() {
	echo "Usage: $0 [options] {start|stop|install|uninstall}"

	echo ""
	echo "Eagle Version: ${EAGLE_VERSION} HDP Version: ${HDP_VERSION}"
	echo ""
	echo "Commands:"
	echo "  start           Start EAGLE components in Ambari"
	echo "  stop            Stop EAGLE components in Ambari"
	echo "  install         Install EAGLE components into Ambari"
	echo "  uninstall       Uninstall EAGLE components from from Ambari"
	echo "Options:"
	echo "  --host          Ambari server host, default: localhost"
	echo "  --port          Ambari server port, default: 8080"
	echo "  --cluster       Ambari cluster name, default: Sandbox"
	echo "  --user          Ambari account username, default: admin"
	echo "  --password      Ambari account password, default: admin"
	echo ""
	exit 1
}

function install(){
	if [ -z "$EAGLE_HOME" ];then
		EAGLE_HOME=$DEFAULT_EAGLE_HOME
	else
		echo "EAGLE_HOME: $EAGLE_HOME"
	fi

	if [ ! -e $EAGLE_HOME ]; then
	    echo "Failure: ${EAGLE_HOME} not exists, please download latest eagle package and install to ${EAGLE_HOME}"
	    exit 1
	fi

	if [ ! -e $AMBARI_SERVER_HOME ];then
	    echo "Failure: $AMBARI_SERVER_HOME not exists, maybe because Ambari server is not installed"
	    exit 1
	fi

	echo "Install EAGLE Ambari Plugin"
	# 1. Install EAGLE service into /var/lib/ambari-server/resources/stacks/HDP/2.0.6/services/EAGLE/
	echo "> Installing $AMBARI_EAGLE_STACK_DIR"
	if [ -e $AMBARI_EAGLE_STACK_DIR ];then
	    echo "${AMBARI_EAGLE_STACK_DIR} already exists, override ..."
	    rm -rf $AMBARI_EAGLE_STACK_DIR
	fi

    mkdir -p $AMBARI_EAGLE_STACK_DIR
    cp lib/metainfo.xml $AMBARI_EAGLE_STACK_DIR

	# 2. Install EAGLE package into /var/lib/ambari-server/resources/common-services/EAGLE/0.0.1/
	echo "> Installing $AMBARI_EAGLE_COMMON_DIR"
	if [ -e $AMBARI_EAGLE_COMMON_DIR ];then
	    echo "${AMBARI_EAGLE_COMMON_DIR} already exists, override ..."
    fi

    mkdir -p $AMBARI_EAGLE_COMMON_DIR
    cp -rf lib/EAGLE/* $AMBARI_EAGLE_COMMON_DIR

	# 3. Install eagle quick links
	echo "> Installing eagle quick links"
	cp lib/EAGLE/package/patches/app.js.gz $AMBARI_SERVER_APP_JS

	# create
	echo ""

	which ambari-server >/dev/null 2>&1
	if [ $? == 0 ];then
		echo "Restarting ambari server"
		ambari-server restart
		echo "Congratulations! you almost finish, please add EAGLE through Ambari UI"
	else
		echo "Congratulations! you almost finish, please restart ambari server by 'ambari-sever restart' and add EAGLE through Ambari UI"
	fi

	exit 0
}

function uninstall(){
	echo "Stopping EAGLE"
	echo ""
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ${AMBARI_USER}' -X PUT -d '{"RequestInfo": {"context" :"Stop EAGLE via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://${AMBARI_HOST}:${AMBARI_PORT}/api/v1/clusters/${AMBARI_CLUSTER_NAME}/services/EAGLE

	echo "Uninstalling EAGLE"
	curl -H "X-Requested-By: $AMBARI_USER" -u $AMBARI_USER:$AMBARI_PASSWORD -X DELETE  http://${AMBARI_HOST}:${AMBARI_PORT}/api/v1/clusters/${AMBARI_CLUSTER_NAME}/services/EAGLE

	echo "Finished to uninstall EAGLE from Ambari"
}

function create(){
	# TODO: Fix bug in service creation

	echo "WARNING: Creating EAGLE service in Ambari, which still has problem"
	echo ""
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ${AMBARI_USER}' -i -X POST -d '{"ServiceInfo":{"service_name":"EAGLE"}}' http://${AMBARI_HOST}:${AMBARI_PORT}/api/v1/clusters/${AMBARI_CLUSTER_NAME}/services
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ${AMBARI_USER}' -i -X POST -d '{"components":[{"ServiceComponentInfo":{"component_name":"EAGLE_SERVICE"}},{"ServiceComponentInfo":{"component_name":"EAGLE_TOPOLOGY"}}]}' http://${AMBARI_HOST}:${AMBARI_PORT}/api/v1/clusters/${AMBARI_CLUSTER_NAME}/services?ServiceInfo/service_name=EAGLE
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ${AMBARI_USER}' -X PUT -d '{"RequestInfo": {"context" :"Installing EAGLE via REST", "operation_level":{"level":"CLUSTER","cluster_name":"$AMABRI_CLUSTER_NAME"}}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' "http://${AMBARI_HOST}:${AMBARI_PORT}/api/v1/clusters/${AMBARI_CLUSTER_NAME}/services?ServiceInfo/service_name.in(EAGLE)"
}

function start(){
	echo "Starting EAGLE"
	echo ""
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ${AMBARI_USER}' -X PUT -d '{"RequestInfo": {"context" :"Start EAGLE via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://${AMBARI_HOST}:${AMBARI_PORT}/api/v1/clusters/${AMBARI_CLUSTER_NAME}/services/EAGLE
}

function stop(){
	echo "Stopping EAGLE"
	echo ""
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ${AMBARI_USER}' -X PUT -d '{"RequestInfo": {"context" :"Stop EAGLE via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://${AMBARI_HOST}:${AMBARI_PORT}/api/v1/clusters/${AMBARI_CLUSTER_NAME}/services/EAGLE
}

case $1 in
"--host")
	if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 AMBARI_HOST=$2
     shift 2
     ;;
"--port")
	if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 AMBARI_PORT=$2
     shift 2
     ;;
"--cluster")
	if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 AMBARI_CLUSTER=$2
     shift 2
     ;;
"--user")
	if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 AMBARI_USER=$2
     shift 2
     ;;
"--password")
	if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 AMBARI_PASSWORD=$2
     shift 2
     ;;
"install")
	install
	exit
	;;
"uninstall")
	uninstall
	exit
	;;
"start")
	start
	exit
	;;
"stop")
	stop
	exit
	;;
*)
	usage
	exit 1
	;;
esac
