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

#####################################################################
#            Import stream metadata for HDFS
#####################################################################

## AlertDataSource: data sources bound to sites
echo "Importing AlertDataSourceService for persist... "

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteApplicationService" \
  -d '
  [
     {
        "tags":{
           "site":"sandbox",
           "application":"hadoopJmxMetricDataSource"
        },
        "enabled": true,
        "config":"{}"
     }
  ]
  '

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=ApplicationDescService" \
  -d '
  [
     {
        "tags":{
           "application":"hadoopJmxMetricDataSource"
        },
        "desc":"hadoop jmx metric monitoring",
        "alias":"JmxMetric",
        "group":"HadoopMetricMonitoring",
        "config":"{}",
        "features":["common","metadata"]
     }
  ]
  '

## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" \
 -d '
 [
    {
       "prefix":"alertStream",
       "tags":{
          "dataSource":"hadoopJmxMetricDataSource",
          "streamName":"hadoopJmxMetricEventStream"
       },
       "desc":"hadoop"
    }
 ]
 '

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" \
 -d '
 [
    {
       "prefix":"alertExecutor",
       "tags":{
          "dataSource":"hadoopJmxMetricDataSource",
          "alertExecutorId":"hadoopJmxMetricAlertExecutor",
          "streamName":"hadoopJmxMetricEventStream"
       },
       "desc":"aggregate executor for hadoop jmx metric event stream"
    }
 ]
 '

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
          "dataSource": "hadoopJmxMetricDataSource",
          "streamName": "hadoopJmxMetricEventStream",
          "attrName": "host"
       },
       "attrDescription": "the host that current metric comes form",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "hadoopJmxMetricDataSource",
          "streamName": "hadoopJmxMetricEventStream",
          "attrName": "timestamp"
       },
       "attrDescription": "the metric timestamp",
       "attrType": "long",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "hadoopJmxMetricDataSource",
          "streamName": "hadoopJmxMetricEventStream",
          "attrName": "metric"
       },
       "attrDescription": "the metric name",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "hadoopJmxMetricDataSource",
          "streamName": "hadoopJmxMetricEventStream",
          "attrName": "component"
       },
       "attrDescription": "the component that the metric comes from",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "hadoopJmxMetricDataSource",
          "streamName": "hadoopJmxMetricEventStream",
          "attrName": "site"
       },
       "attrDescription": "the site that the metric belongs to",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "prefix": "alertStreamSchema",
       "tags": {
          "dataSource": "hadoopJmxMetricDataSource",
          "streamName": "hadoopJmxMetricEventStream",
          "attrName": "value"
       },
       "attrDescription": "the metric value in string presentation",
       "attrType": "double",
       "category": "",
       "attrValueResolver": ""
    }
 ]
 '

## Finished
echo ""
echo "Finished initialization for eagle topology"