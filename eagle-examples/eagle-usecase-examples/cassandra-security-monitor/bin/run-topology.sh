#!/usr/bin/env bash

export EAGLE_BASE_DIR=$(dirname $0)/../../../../
export EAGLE_ASSEMBLY_TARGET=${EAGLE_BASE_DIR}/eagle-assembly/target/eagle-*-bin/eagle-*/

ls ${EAGLE_ASSEMBLY_TARGET} 1>/dev/null 2>/dev/null
if [ "$?" != "0" ];then
	echo "$EAGLE_ASSEMBLY_TARGET not exist, build now"
	mvn clean package -DskipTests
fi

chmod +x $(dirname $0)/*.sh

$(dirname $0)/init-metadata.sh
$(dirname $0)/import-policy.sh

$EAGLE_ASSEMBLY_TARGET/bin/kafka-stream-monitor.sh cassandraQueryLogStream cassandraQueryLogExecutor $(dirname $0)/../conf/cassandra-querylog-local.conf