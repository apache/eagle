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

echo "Importing AlertDataSourceService for HBase... "

#### AlertStreamService: alert streams generated from data source

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream"},"desc":"alert event stream from hbase security audit log"}]'


#### AlertExecutorService: what alert streams are consumed by alert executor

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"hbaseSecurityLog","alertExecutorId":"hbaseSecurityLogAlertExecutor","streamName":"hbaseSecurityLogEventStream"},"desc":"alert executor for hbase security log event stream"}]'


#### AlertStreamSchemaService: schema for event from alert stream

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"host"},"attrDescription":"remote ip address to access hbase","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"request"},"attrDescription":"","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseRequestResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"status"},"attrDescription":"access status: allowed or denied","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"user"},"attrDescription":"hbase user","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"timestamp"},"attrDescription":"milliseconds of the datetime","attrType":"long","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"scope"},"attrDescription":"the resources which users are then granted specific permissions (Read, Write, Execute, Create, Admin) against","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseMetadataResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"action"},"attrDescription":"action types, such as read, write, create, execute, and admin","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseActionResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"sensitivityType"},"attrDescription":"","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseSensitivityTypeResolver"}]'

echo ""

## AlertDataSource: data sources bound to sites
echo "Importing AlertDataSourceService for hbase... "

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site":"sandbox","dataSource":"hbaseSecurityLog"},"enabled":"true","config":"{\"zkClientPort\":\"2181\", \"zkQuorum\":\"localhost\"}","desc":"HBASE"}]'