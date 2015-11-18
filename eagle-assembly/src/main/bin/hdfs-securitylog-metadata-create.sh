#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with`
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

echo "Importing metadata for HDFS security log... "

#### AlertStreamService: alert streams generated from data source

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"hdfsSecurityLog","streamName":"hdfsSecurityLogEventStream"},"desc":"alert event stream from HDFS security audit log"}]'


#### AlertExecutorService: what alert streams are consumed by alert executor

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"hdfsSecurityLog","alertExecutorId":"hdfsSecurityLogAlertExecutor","streamName":"hdfsSecurityLogEventStream"},"desc":"alert executor for HDFS security log event stream"}]'


#### AlertStreamSchemaService: schema for event from alert stream

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsSecurityLog","streamName":"hdfsSecurityLogEventStream","attrName":"timestamp"},"attrDescription":"milliseconds of the datetime","attrType":"long","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsSecurityLog","streamName":"hdfsSecurityLogEventStream","attrName":"user"},"attrDescription":"","attrType":"string","category":"","attrValueResolver":"eagle.service.security.hbase.resolver.HbaseRequestResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsSecurityLog","streamName":"hdfsSecurityLogEventStream","attrName":"allowed"},"attrDescription":"true, false or none","attrType":"bool","category":"","attrValueResolver":""}]'

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site":"sandbox","dataSource":"hdfsSecurityLog"},"enabled":"true","desc":"HDFS Security"}]'


echo ""

