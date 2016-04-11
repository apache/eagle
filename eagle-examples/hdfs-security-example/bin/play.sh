#!/usr/bin/env bash

export EAGLE_BASE_DIR=$(dirname $0)/../../../../
export EAGLE_ASSEMBLY_TARGET=${EAGLE_BASE_DIR}/eagle-assembly/target/eagle-*-bin/eagle-*/

ls ${EAGLE_ASSEMBLY_TARGET} 1>/dev/null 2>/dev/null
if [ "$?" != "0" ];then
	echo "$EAGLE_ASSEMBLY_TARGET not exist, build now"
	cd $EAGLE_BASE_DIR
	mvn package -DskipTests
fi

cd $(dirname $0)/../

$EAGLE_ASSEMBLY_TARGET/bin/eagle-service.sh status

if [ "$?" != "0" ];then
	echo "Starting eagle service ..."
	$EAGLE_ASSEMBLY_TARGET/bin/eagle-service.sh start
	echo "Wait 10 seconds for eagle service to be ready .. "
	sleep 10
else
	echo "Eagle service has already started"
fi

chmod +x $(dirname $0)/*.sh

$(dirname $0)/initialize.sh

$EAGLE_ASSEMBLY_TARGET/bin/kafka-stream-monitor.sh cassandraQueryLogStream cassandraQueryLogExecutor $(dirname $0)/../conf/cassandra-querylog-local.conf