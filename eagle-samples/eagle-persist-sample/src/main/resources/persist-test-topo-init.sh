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
eagle_bin=$EAGLE_HOME/bin

#####################################################################
#            Import stream metadata for HDFS
#####################################################################

## AlertDataSource: data sources bound to sites
echo "Importing AlertDataSourceService for persist... "

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" \
  -d '[{"prefix":"alertDataSource","tags":{"site" : "sandbox", "dataSource":"persistTest"}, "enabled": "true", "config" : " just some description", "desc":"persistTest"}]'


## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" \
 -d '[{"prefix":"alertStream","tags":{"dataSource":"persistTest","streamName":"persistTestEventStream"},"desc":"persistTest metrics"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" \
 -d '[{"prefix":"alertExecutor","tags":{"dataSource":"persistTest","alertExecutorId":"aggregateExecutor1","streamName":"persistTestEventStream"},"desc":"aggregate executor for persist test log event stream"}]'

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
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "src"
       },
       "attrDescription": "source directory or file, such as /tmp",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver"
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "dst"
       },
       "attrDescription": "destination directory, such as /tmp",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver"
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "host"
       },
       "attrDescription": "hostname, such as localhost",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "timestamp"
       },
       "attrDescription": "milliseconds of the datetime",
       "attrType": "long",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "allowed"
       },
       "attrDescription": "true, false or none",
       "attrType": "bool",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "user"
       },
       "attrDescription": "process user",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "cmd"
       },
       "attrDescription": "file/directory operation, such as getfileinfo, open, listStatus and so on",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.HDFSCommandResolver"
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "sensitivityType"
       },
       "attrDescription": "mark such as AUDITLOG, SECURITYLOG",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.HDFSSensitivityTypeResolver"
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "persistTest",
          "streamName": "persistTestEventStream",
          "attrName": "securityZone"
       },
       "attrDescription": "",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    }
 ]
 '

##### add policies ##########
echo ""
echo "Importing AlertStreamSchemaService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AggregateDefinitionService" \
 -d '
 [
     {
       "prefix": "aggregatedef",
       "tags": {
         "site": "sandbox",
         "dataSource": "persistTest",
         "policyId": "persitTestPolicy1",
         "aggregateExecutorId": "aggregateExecutor1",
         "policyType": "siddhiCEPEngine"
       },
       "desc": "persistetest",
       "policyDef": "{\"expression\":\"from persistTestEventStream[(logLevel == 'ERROR')] select * insert into outputStream;\",\"type\":\"siddhiCEPEngine\"}",
       "enabled": true
     }
 ]
 '



 ## Finished
echo ""
echo "Finished initialization for eagle topology"

exit 0
