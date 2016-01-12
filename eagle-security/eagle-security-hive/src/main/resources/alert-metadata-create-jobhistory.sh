#!/bin/sh
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

#### AlertStreamService: alert streams generated from data source
curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream"},"desc":"alert event stream from hive query"}]'

#### AlertExecutorService: what alert streams are consumed by alert executor
curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"hiveQueryLog","alertExecutorId":"hiveAccessAlertByJobHistory","streamName":"hiveAccessLogStream"},"desc":"alert executor for hive query log event stream"}]'

#### AlertStreamSchemaService: schema for event from alert stream
curl -X POST -H 'Content-Type:application/json' "http://localhost:38080/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"user"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"command"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"timestamp"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"resource"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"sensitivityType"}}]'
