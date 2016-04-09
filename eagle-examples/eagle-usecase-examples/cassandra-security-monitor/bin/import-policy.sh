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
echo "Importing policy: capacityUsedPolicy "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
 -d '
 [
     {
		"tags": {
			"site": "sandbox",
			"application": "cassandraQueryLog",
			"policyId": "cf_ customer_details_and_ ks_ dg_keyspace_policy",
			"alertExecutorId": "cassandraQueryLogExecutor",
			"policyType": "siddhiCEPEngine"
		},
		"description": "Alert[cf_ customer_details_and_ ks_ dg_keyspace_policy]",
		"policyDef": "{\"expression\":\"from cassandraQueryLogStream[(cf == \\\"customer_details\\\") and (ks == \\\"dg_keyspace\\\")] select * insert into outputStream;\",\"type\":\"siddhiCEPEngine\"}",
		"dedupeDef": "{\"alertDedupIntervalMin\": 10,\"emailDedupIntervalMin\": 10}",
		"notificationDef": "[{\"sender\":\"no-reply@eagle.incubator.apache.org\",\"recipients\":\"no-reply@eagle.incubator.apache.org\",\"subject\":\"Alert[cf_ customer_details_and_ ks_ dg_keyspace_policy]\",\"flavor\":\"email\",\"id\":\"email_1\",\"tplFileName\":\"\"}]",
		"remediationDef": "",
		"enabled": true,
		"owner": "admin",
		"severity": 0,
		"markdownEnabled": false
	}
 ]'

 ## Finished
echo ""
echo "Finished initialization for eagle topology"

exit 0
