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

#####################################################################
#            Import stream metadata for HADOOP METRIC
#####################################################################
## AlertDataSource: data sources bound to sites
echo "Importing AlertDataSourceService for HADOOP METRIC ... "

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site" : "sandbox", "dataSource":"hadoop"}, "enabled": "true", "config" : "", "desc":"HADOOP"}]'


## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HADOOP METRIC ... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST \
-H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" \
-d '[{"prefix":"alertStream","tags":{"dataSource":"hadoop","streamName":"hadoopJmxMetric"},"desc":"hadoop jmx metric stream"},{"prefix":"alertStream","tags":{"dataSource":"hadoop","streamName":"hadoopSysMetric"},"desc":"hadoop system metric stream"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HADOOP METRIC ... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"hadoop","alertExecutorId":"hadoopJmxMetricExecutor","streamName":"hadoopJmxMetric"},"desc":"alert executor for hadoop jmx stream"}]'
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"hadoop","alertExecutorId":"hadoopSysMetricExecutor","streamName":"hadoopSysMetric"},"desc":"alert executor for hadoop system stream"}]'

## AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HADOOP METRIC ... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"tags":{"dataSource":"hadoop","attrName":"host","streamName":"hadoopJmxMetric"},"attrType":"string","attrDescription":"metric host","attrDisplayName":"host"},{"tags":{"dataSource":"hadoop","attrName":"site","streamName":"hadoopJmxMetric"},"attrType":"string","attrDescription":"metric site","attrDisplayName":"site"},{"tags":{"dataSource":"hadoop","attrName":"timestamp","streamName":"hadoopJmxMetric"},"attrType":"long","attrDescription":"metric timestamp","attrDisplayName":"timestamp"},{"tags":{"dataSource":"hadoop","attrName":"value","streamName":"hadoopJmxMetric"},"attrType":"string","attrDescription":"metric value","attrDisplayName":"value"},{"tags":{"dataSource":"hadoop","attrName":"component","streamName":"hadoopJmxMetric"},"attrType":"string","attrDescription":"service component","attrDisplayName":"component"},{"tags":{"dataSource":"hadoop","attrName":"metric","streamName":"hadoopJmxMetric"},"attrType":"string","attrDescription":"metric name","attrDisplayName":"metric"}]'
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"tags":{"dataSource":"hadoop","attrName":"host","streamName":"hadoopSysMetric"},"attrType":"string","attrDescription":"metric host","attrDisplayName":"host"},{"tags":{"dataSource":"hadoop","attrName":"site","streamName":"hadoopSysMetric"},"attrType":"string","attrDescription":"metric site","attrDisplayName":"site"},{"tags":{"dataSource":"hadoop","attrName":"timestamp","streamName":"hadoopSysMetric"},"attrType":"long","attrDescription":"metric timestamp","attrDisplayName":"timestamp"},{"tags":{"dataSource":"hadoop","attrName":"value","streamName":"hadoopSysMetric"},"attrType":"string","attrDescription":"metric value","attrDisplayName":"value"},{"tags":{"dataSource":"hadoop","attrName":"component","streamName":"hadoopSysMetric"},"attrType":"string","attrDescription":"service component","attrDisplayName":"component"},{"tags":{"dataSource":"hadoop","attrName":"metric","streamName":"hadoopSysMetric"},"attrType":"string","attrDescription":"metric name","attrDisplayName":"metric"}]'

$EAGLE_HOME/kafka-stream-monitor.sh hadoopJmxMetric hadoopJmxMetricExecutor $EAGLE_HOME/conf/sandbox-hadoopjmx-topology.conf