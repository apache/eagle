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

##### add policies ##########
echo ""
echo "Creating Meta Data for GC Monitoring  ..."


#### AlertDataSourceService: alert streams generated from data source
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteApplicationService" \
  -d '
  [
     {
        "tags":{
           "site":"sandbox",
           "application":"NNGCLog"
        },
        "enabled": true        
     }
  ]
  '
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=ApplicationDescService" \
  -d '
  [
     {
        "tags":{
           "application":"NNGCLog"
        },
        "desc":"hadoop gc log monitoring",
        "alias":"NNGCLogMonitor",
        "group":"DAM",
        "config":"{}",
        "features":["common","metadata"]
     }
  ]
  '
  
#### AlertStreamService: alert streams generated from data source
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream"},"desc":"alert event stream from namenode gc log"}]'

#### AlertExecutorService: what alert streams are consumed by alert executor
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"NNGCLog","alertExecutorId":"NNGCAlert","streamName":"NNGCLogStream"},"desc":"alert executor for namenode gc log"}]'

#### AlertStreamSchemaService: schema for event from alert stream
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"timestamp"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"eventType"}},{"prefix":"alertStreamSchema","category":"","attrType":"double","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"pausedGCTimeSec"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"youngAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"youngUsedHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"youngTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"permAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"permUsedHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"permTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"totalHeapUsageAvailable"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"usedTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredUsedHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"totalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"logLine"}}]'


echo " Initialized GC Log Monitoring ...."


