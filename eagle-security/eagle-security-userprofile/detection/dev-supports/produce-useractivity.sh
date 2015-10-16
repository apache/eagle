#!/usr/bin/env bash

/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list sandbox.hortonworks.com:6667 --topic sandbox_hdfs_audit_agg
