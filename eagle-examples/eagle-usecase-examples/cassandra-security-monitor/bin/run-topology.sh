#!/usr/bin/env bash

export EAGLE_ASSEMBLY_TARGET=$(dirname $0)/../../../../eagle-assembly/target/eagle-*-bin/eagle-*/

chmod +x $(dirname $0)/*.sh

$(dirname $0)/init-metadata.sh
$(dirname $0)/import-policy.sh

$EAGLE_ASSEMBLY_TARGET/bin/kafka-stream-monitor.sh cassandraQueryLogStream cassandraQueryLogExecutor $(dirname $0)/../conf/cassandra-querylog-local.conf