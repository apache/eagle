#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#!/bin/sh
#### AlertDataSourceService: alert streams generated from data source
curl -X POST -H 'Content-Type:application/json' -H 'Authorization: Basic YWRtaW46c2VjcmV0' "http://localhost:9099/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix": "alertDataSource", "tags": {"site": "sandbox","dataSource": "NNGCLog"}}]'

#### AlertStreamService: alert streams generated from data source
curl -X POST -H 'Content-Type:application/json' -H 'Authorization: Basic YWRtaW46c2VjcmV0' "http://localhost:9099/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream"},"desc":"alert event stream from namenode gc log"}]'

#### AlertExecutorService: what alert streams are consumed by alert executor
curl -X POST -H 'Content-Type:application/json' -H 'Authorization: Basic YWRtaW46c2VjcmV0' "http://localhost:9099/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"NNGCLog","alertExecutorId":"NNGCAlert","streamName":"NNGCLogStream"},"desc":"alert executor for namenode gc log"}]'

#### AlertStreamSchemaService: schema for event from alert stream
curl -X POST -H 'Content-Type:application/json' -H 'Authorization: Basic YWRtaW46c2VjcmV0' "http://localhost:9099/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"timestamp"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"eventType"}},{"prefix":"alertStreamSchema","category":"","attrType":"double","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"pausedGCTimeSec"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"youngAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"youngUsedHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"youngTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"permAreaGCed"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"permUsedHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"permTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"totalHeapUsageAvailable"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"usedTotalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"bool","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"tenuredUsedHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"totalHeapK"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrValueResolver":"","tags":{"dataSource":"NNGCLog","streamName":"NNGCLogStream","attrName":"logLine"}}]'