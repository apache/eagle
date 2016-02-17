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

CUR_DIR=$(dirname $0)
source $CUR_DIR/../../../../../eagle-assembly/src/main/bin/eagle-env.sh

#####################################################################
#            Import stream metadata
#####################################################################

## AlertDataSource: data sources bound to sites
echo "Importing AlertDataSourceService for persist... "

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" \
  -d '[{"prefix":"alertDataSource","tags":{"site" : "sandbox", "dataSource":"testSpout"}, "enabled": "true", "config" : " just some description", "desc":"pluginTest"}]'


## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" \
 -d '[{"prefix":"alertStream","tags":{"dataSource":"testSpout","streamName":"testStream"},"desc":"pluginTest"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" \
 -d '[{"prefix":"alertExecutor","tags":{"dataSource":"testSpout","alertExecutorId":"testExecutor","streamName":"testStream"},"desc":"testStream->testExecutor"}]'

## AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
"http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" \
 -d '
 [
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "testSpout",
          "streamName": "testStream",
          "attrName": "testAttribute"
       },
       "attrDescription": "testAttribute",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    }
 ]
 '

##### add policies ##########
echo ""
echo "Importing policy ... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
 -d '
 [
     {
       "prefix": "alertdef",
       "tags": {
         "site": "sandbox",
         "dataSource": "testSpout",
         "policyId": "pluginTestPolicy",
         "alertExecutorId": "testExecutor",
         "policyType": "siddhiCEPEngine"
       },
       "description": "pluginTest",
       "policyDef": "{\"expression\":\"from testStream[(testAttribute == \\\"testValue\\\")] select * insert into outputStream;\",\"type\":\"siddhiCEPEngine\"}",
       "dedupeDef": "{\"alertDedupIntervalMin\":0,\"emailDedupIntervalMin\":1440}",
       "notificationDef": "[{\"subject\":\"just for test\",\"sender\":\"nobody@test.com\",\"recipients\":\"nobody@test.com\",\"flavor\":\"email\",\"id\":\"email_1\",\"tplFileName\":\"\"}]",
       "remediationDef":"",
       "enabled":true
     }
 ]
 '

## Finished
echo ""
echo "Finished initialization for NotificationPluginTest"

exit 0
