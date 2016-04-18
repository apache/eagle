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

function print_help() {
	echo "Usage: $0 {start | stop | restart | status}"
	exit 1
}

if [ $# != 1 ]
then
	print_help
fi

source $(dirname $0)/eagle-env.sh

export CATALINA_HOME=$EAGLE_HOME/lib/tomcat
export CATALINA_BASE=$CATALINA_HOME

export CATALINA_LOGDIR=$EAGLE_HOME/logs
export CATALINA_TMPDIR=$EAGLE_HOME/temp
export CATALINA_OUT=$CATALINA_LOGDIR/eagle-service.out
export CATALINA_PID=$CATALINA_TMPDIR/service.pid
export JAVA_OPTS=-Xmx3072m -XX:MaxPermSize=1024m

# CLASSPATH
export CLASSPATH=$CLASSPATH:$EAGLE_HOME/conf

#for i in `ls $EAGLE_HOME/lib/*.jar`; do CLASSPATH=$CLASSPATH:$i; done

if [ ! -e $CATALINA_LOGDIR ];then
    mkdir -p $CATALINA_LOGDIR
fi

if [ ! -e $CATALINA_TMPDIR ]; then
    mkdir -p $CATALINA_TMPDIR
fi


EAGLE_SERVICE_CONF="eagle-service.conf"
EAGLE_LDAP_CONF="ldap.properties"
EAGLE_SCHEDULER_CONF="eagle-scheduler.conf"

# Always copy conf/eagle-service.properties to lib/tomcat/webapps/eagle-service/WEB-INF/classes/application.conf before starting
if [ ! -e ${EAGLE_HOME}/conf/${EAGLE_SERVICE_CONF} ]
then
	echo "Failure: cannot find ${EAGLE_HOME}/conf/${EAGLE_SERVICE_CONF}"
	exit 1
fi
cp -f $EAGLE_HOME/conf/$EAGLE_SERVICE_CONF ${EAGLE_HOME}/lib/tomcat/webapps/eagle-service/WEB-INF/classes/application.conf

if [ -e ${EAGLE_HOME}/conf/${EAGLE_LDAP_CONF} ]
then
	cp -f $EAGLE_HOME/conf/$EAGLE_LDAP_CONF ${EAGLE_HOME}/lib/tomcat/webapps/eagle-service/WEB-INF/classes/
fi
if [ -e ${EAGLE_HOME}/conf/${EAGLE_SCHEDULER_CONF} ]
then
	cp -f $EAGLE_HOME/conf/$EAGLE_SCHEDULER_CONF ${EAGLE_HOME}/lib/tomcat/webapps/eagle-service/WEB-INF/classes/
fi


case $1 in
"start")
	echo "Starting eagle service ..."
	$EAGLE_HOME/lib/tomcat/bin/catalina.sh start
	if [ $? != 0 ];then 
		echo "Error: failed starting"
		exit 1
	fi
	;;
"stop")
	echo "Stopping eagle service ..."
	$EAGLE_HOME/lib/tomcat/bin/catalina.sh stop
	if [ $? != 0 ];then
		echo "Error: failed stopping"
		exit 1
	fi
	echo "Stopping is completed"
	;;
"restart")
	echo "Stopping eagle service ..."
	$EAGLE_HOME/lib/tomcat/bin/catalina.sh stop
	echo "Restarting eagle service ..."
	$EAGLE_HOME/lib/tomcat/bin/catalina.sh start
	if [ $? != 0 ];then
		echo "Error: failed starting"
		exit 1
	fi
	echo "Restarting is completed "
	;;
"status")
	#echo "Listing eagle service status ..."
	if [ -e $CATALINA_TMPDIR/service.pid ]  && ps -p `cat $CATALINA_TMPDIR/service.pid`  > /dev/null
	then
		echo "Eagle service is running `cat $CATALINA_TMPDIR/service.pid`"
		exit 0
	else
		echo "Eagle service is stopped"
		exit 1
	fi
	;;
*)
	print_help
	;;
esac

if [ $? != 0 ]; then
	echo "Error: start failure"
	exit 1
fi

exit 0

