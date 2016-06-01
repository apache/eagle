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
source $CUR_DIR/../../../../../../eagle-assembly/src/main/bin/eagle-env.sh

#####################################################################
#     Import notification plugin configuration into Eagle Service   #
#####################################################################

## AlertNotificationService : schema for notifcation plugin configuration
echo ""
echo "Importing notification plugin configurations ... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertNotificationService" \
 -d '
 [
     {
       "prefix": "alertNotifications",
       "tags": {
         "notificationType": "email"
       },
       "className": "org.apache.eagle.notification.plugin.AlertEmailPlugin",
       "description": "send alert to email",
       "enabled":true
     },
     {
       "prefix": "alertNotifications",
       "tags": {
         "notificationType": "kafka"
       },
       "className": "org.apache.eagle.notification.plugin.AlertKafkaPlugin",
       "description": "send alert to kafka bus",
       "enabled":true
     },
     {
       "prefix": "alertNotifications",
       "tags": {
         "notificationType": "eagleStore"
       },
       "className": "org.apache.eagle.notification.plugin.AlertEagleStorePlugin",
       "description": "send alert to eagle store",
       "enabled":true
     }
 ]
 '

## Finished
echo ""
echo "Finished initialization for alert notification plugins"

exit 0
