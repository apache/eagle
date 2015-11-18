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

source $(dirname $0)/../bin/eagle-env.sh

#### create hive sensitivity sample in sandbox
echo "create hive sensitivity sample in sandbox... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=HiveResourceSensitivityService" -d '[{"tags":{"site" : "sandbox", "hiveResource":"/xademo/customer_details/phone_number"}, "sensitivityType": "PHONE_NUMBER"}]'


#### create hdfs sensitivity sample in sandbox
echo "create hdfs sensitivity sample in sandbox... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=FileSensitivityService" -d '[{"tags":{"site" : "sandbox", "filedir":"/tmp/private"}, "sensitivityType": "PRIVATE"}]'

#### create hbase sensitivity sample in sandbox
echo "create hdfs sensitivity sample in sandbox... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=HbaseResourceSensitivityService" -d '[{"tags":{"site":"sandbox","hbaseResource":"default:alertStreamSchema"},"sensitivityType":"PrivateTable"}]'

